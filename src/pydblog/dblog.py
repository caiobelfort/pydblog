"""
The DBLog algorithm, driven over a ``SourceConnector``.

``DBLog`` owns the algorithm; the connector owns the database. The attribute holding
the connector is typed as the Protocol rather than as any concrete class, so the
algorithm can only ever reach for the primitives every source must provide — a second
source type needs no change here.
"""

import logging
from datetime import datetime
from types import TracebackType

from polars import DataFrame, concat

from pydblog.connectors.base import SourceConnector, build_connector
from pydblog.connectors.types import LSN, SourceType, TableSpec
from pydblog.log import configure_logging
from pydblog.state import DumpState, JsonFileStore, StateStore

DEFAULT_CHUNK_SIZE = 1000

logger = logging.getLogger(__name__)


class CdcRetentionExpiredError(RuntimeError):
    """
    Raised when a run is asked to start below what the change log still retains.

    Its own type because the recovery is specific: the gap cannot be filled from the
    log, so the caller has to start a fresh dump rather than retry the read.
    """


def _check_arguments(dump: str | None, from_lsn: LSN | None) -> None:
    """
    Refuse a run that cannot be started, before anything is built.

    Neither of these needs the database to decide, so both are settled at construction
    rather than on the first fetch, when a typo has already cost a round trip.

    Raises:
        ValueError: If ``dump`` is blank, or if neither a ``dump`` nor a ``from_lsn``
            was given.
    """

    if dump is not None and not dump.strip():
        raise ValueError("dump name is blank: it is the key progress is kept under")

    if dump is None and from_lsn is None:
        raise ValueError(
            "a run with no dump needs from_lsn: with no chunks to establish "
            "current state there is no safe default, since starting at the "
            "retention floor replays history and starting at the present drops "
            "it, and neither is visible to the caller"
        )


def _recorded(
    store: StateStore, dump: str | None, spec: TableSpec
) -> DumpState | None:
    """
    The progress recorded for a dump, if there is any to pick up.

    Raises:
        ValueError: If the dump last ran against a different table, whose chunk key is
            a position in a key space this one does not share.
    """

    if dump is None:
        return None

    recorded = store.load(dump)
    if recorded is not None and recorded.table != spec.qualified_name:
        raise ValueError(
            f"dump {dump!r} last ran against {recorded.table}, not "
            f"{spec.qualified_name}: its chunk key is a position in a key space "
            "this table does not share"
        )

    return recorded


def _opening_lsn(
    connector: SourceConnector,
    spec: TableSpec,
    dump: str | None,
    from_lsn: LSN | None,
    recorded: DumpState | None,
    floor: LSN,
) -> LSN:
    """
    Where the run's first window opens.

    Recorded progress wins over anything the caller suggests: its chunk key and its LSN
    were written together and only mean anything together, so honouring a ``from_lsn``
    against a resumed chunk key would leave a gap between them. Failing that an
    explicit ``from_lsn`` wins, and failing that a dump opens at the present, because
    its chunks carry the table as it is now and replaying the log's whole retention
    window would be work that changes nothing.

    A run with no dump has been made to supply a ``from_lsn`` by now, so the last case
    is the floor only in principle.

    Raises:
        CdcRetentionExpiredError: If the position lands below what the log retains.
    """

    if recorded is not None:
        opening = recorded.last_lsn
    elif from_lsn is not None:
        opening = from_lsn
    elif dump is not None:
        # The floor matters here: a capture instance enabled moments ago has a start
        # LSN the database-wide max has not reached, and opening at the bare max would
        # sit below what the log retains.
        opening = max(connector.get_max_lsn(), floor)
    else:
        opening = floor

    if opening < floor:
        raise CdcRetentionExpiredError(
            f"{spec.qualified_name} no longer retains {opening.hex()}: the log "
            f"starts at {floor.hex()}. The gap cannot be read, so the table has "
            "to be dumped again."
        )

    return opening


class DBLog:
    """
    Interleaves a chunked table dump with the ongoing change event log.

    The dump walks the table in chunks of ``chunk_size`` rows, keyed off the leading
    primary key column. Around each chunk sits a window of log events; events in that
    window win over the chunk's rows, since they are the newer image. The result is a
    single stream where every row is delivered at least once and never stale.

    An instance is one run: what it reads is settled at construction and ``fetch``
    hands back one batch of it per call, carrying the position between calls. Reading a
    second table, or the same one under a second dump name, is a second instance. That
    position is only in memory — what survives a process is whatever the caller
    committed.
    """

    def __init__(
        self,
        source_type: SourceType | str,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        table: str,
        dump: str | None = None,
        from_lsn: LSN | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        state_store: StateStore | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> None:
        """
        Build the connector for the source and settle what the run is going to read.

        The table and the dump identify the run, not any one batch of it, so they are
        fixed here: an instance is one run, and ``fetch`` takes no arguments. Reading a
        second table, or the same table under a second dump name, is a second instance.

        Args:
            source_type: Which source to talk to; selects the connector.
            host: Source hostname.
            port: Source port.
            user: Username to authenticate with.
            password: Password to authenticate with.
            database: Database to read from.
            schema: Schema of the table to read.
            table: Name of the table to read.
            dump: Identity of the dump to run or resume, or None to read the change
                log alone. Progress is recorded under this name, so reusing it carries
                on where that dump stopped and a new name starts over.
            from_lsn: Where to open the first window. None means the present for a
                dump starting fresh. Required without a dump, and ignored once the
                dump has progress recorded.
            chunk_size: Rows per dump chunk. Bounds both memory per chunk and the
                width of the log window bracketing it.
            state_store: Where dump progress is kept, so an interrupted dump can be
                picked up. Defaults to JSON files in the working directory.
            verbose: Report every step to stderr, down to the generated SQL. Off, the
                package stays silent and an application's own logging setup governs
                what, if anything, comes out.
            **kwargs: Passed through to the connector unchanged.

        Raises:
            ValueError: If ``chunk_size`` is not at least 1, if ``dump`` is blank, or
                if neither a ``dump`` nor a ``from_lsn`` was given.
        """

        if chunk_size < 1:
            raise ValueError(f"chunk_size must be at least 1, got {chunk_size}")

        _check_arguments(dump, from_lsn)

        if verbose:
            configure_logging(verbose=True)

        self._connector: SourceConnector = build_connector(
            source_type=source_type,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **kwargs,
        )
        self._chunk_size = chunk_size
        self._store: StateStore = (
            state_store if state_store is not None else JsonFileStore()
        )

        # What this instance reads. Fixed for its lifetime.
        self._schema = schema
        self._table = table
        self._dump = dump
        self._from_lsn = from_lsn

        # Run state, seeded on the first fetch and mutated as the run goes.
        self._spec: TableSpec | None = None  # table being dumped, from inspect()
        self._last_lsn: LSN | None = None  # log position reached; next window opens here
        self._chunk_key: int | None = None  # leading PK the next chunk starts at
        self._dump_done: bool = False  # set once a chunk comes back short
        self._started: bool = False  # whether _start() has run, so it runs once

    @property
    def last_lsn(self) -> LSN | None:
        """
        The log position the run has reached, or None before it starts.

        This is the handoff: passing it back as ``from_lsn`` on a later run picks up
        exactly where this one stopped, with nothing repeated and nothing skipped.
        """

        return self._last_lsn

    @property
    def dump_done(self) -> bool:
        """
        Whether there is any of the table left to walk.

        False before the first ``fetch``. Once it is True, further calls to ``fetch``
        read the log alone — a batch is a window and nothing more — so this is what a
        caller that wants the table and not an open-ended tail loops on:

        ```python
        while not dblog.dump_done:
            frame = dblog.fetch()
            ...
        ```

        Safe to test before fetching because an instance is one run: it starts False
        and only that run's own progress moves it. A recorded dump that had already
        finished shows False until the first fetch loads that, which costs the loop one
        turn and reads a window it was going to want anyway.

        **True from the start for a run with no dump**, which has no table walk to
        finish. That keeps the loop above terminating rather than spinning forever on a
        condition nothing could ever satisfy — but it also means the loop does nothing
        at all for such a run, which is the honest answer to "walk the table" when
        there is no dump. A log-only caller wants the other shape, ``while
        (frame := dblog.fetch()) is not None``, or a poll of its own.
        """

        return self._dump is None or self._dump_done

    def _start(self) -> None:
        """
        Seed the run state for the table, picking up recorded progress if there is any.

        A named dump that has run before starts from what it recorded rather than
        from anywhere the caller suggests: its chunk key and its LSN were written
        together and only mean anything together, so honouring a ``from_lsn`` against
        a resumed chunk key would leave a gap between them.

        Otherwise a dump starting for the first time opens at the present, because its
        chunks carry the table as it is now and replaying the log's whole retention
        window would be work that changes nothing. That is the only default there is:
        a run with no dump has to be given a ``from_lsn``, since with no chunks to
        establish current state either choice loses something the caller cannot see.

        The present is the later of the source's max LSN and the retention floor. The
        floor is the higher of the two on a capture instance enabled moments ago, whose
        start LSN the database-wide max has not reached yet.

        Note that the CDC read is inclusive, so a dump opening at the max LSN sees the
        event sitting exactly on it a second time. That is the safe direction: the
        merge drops it, whereas opening one LSN later would lose a write committed at
        that moment.

        Raises:
            ValueError: If the table has no capture instance to read a log from, or
                the dump last ran against a different table.
            CdcRetentionExpiredError: If the position to start from is below the
                retention floor.
        """

        spec = self._connector.inspect(self._schema, self._table)
        if spec.capture_instance is None:
            raise ValueError(
                f"{spec.qualified_name} has no capture instance: nothing to read a "
                "change log from"
            )

        floor = self._connector.get_min_lsn(spec.capture_instance)
        recorded = _recorded(self._store, self._dump, spec)
        start = _opening_lsn(
            self._connector, spec, self._dump, self._from_lsn, recorded, floor
        )

        self._spec = spec
        self._last_lsn = start
        self._chunk_key = recorded.chunk_key if recorded is not None else None
        self._dump_done = recorded.done if recorded is not None else False

        logger.info(
            f"started {'resumed ' if recorded is not None else ''}run on "
            f"{spec.qualified_name} dump={self._dump} from_lsn={start.hex()}"
        )
        logger.debug(
            f"run state: chunk_key={self._chunk_key} dump_done={self._dump_done} "
            f"floor={floor.hex()} capture_instance={spec.capture_instance}"
        )

    def commit(self) -> None:
        """
        Record how far the dump has got, so an interruption costs one chunk.

        Call this once the frames yielded so far are durably written, and not
        before: it is the caller's declaration that those frames will not need to be
        read again. A run records nothing on its own, so frames that were received
        but never committed are read again on the next run under the same dump name
        — which is the point. Committing on the strength of having merely received a
        frame gives that guarantee up, since a downstream write can still fail after
        the frame is in hand, and the rows behind the recorded position are then
        unreachable: past ``chunk_key`` for the table, past ``last_lsn`` for the log.

        The table position and the log position go down together: a resume needs both
        to agree, and either one on its own would leave rows that neither the
        remaining chunks nor the remaining log would deliver.

        Records the position reached by every frame yielded so far, not one frame in
        particular, so committing per frame and committing per batch of them are both
        sound — the difference is only how much is re-read after a failure.

        Worth one call after the loop as well as inside it. A short chunk both ends the
        dump and arrives as a batch, so committing that batch records the dump as
        finished — but a table whose row count is an exact multiple of ``chunk_size``
        has no short chunk, and its end is found by a batch that comes back empty with
        nothing to commit alongside. Without that last call such a dump is never
        recorded as done, and the next run re-reads the empty page to find out for
        itself: a round trip, and no threat to correctness.

        Does nothing for a run with no dump to record progress under, and nothing
        before a run has started, so a consumer handling both kinds of run can call
        it unconditionally.
        """

        if self._dump is None or self._spec is None or self._last_lsn is None:
            return

        self._store.save(
            DumpState(
                dump=self._dump,
                table=self._spec.qualified_name,
                last_lsn=self._last_lsn,
                chunk_key=self._chunk_key,
                done=self._dump_done,
            )
        )

        logger.debug(
            f"recorded dump {self._dump!r}: chunk_key={self._chunk_key} "
            f"last_lsn={self._last_lsn.hex()} done={self._dump_done}"
        )

    def fetch(self) -> DataFrame | None:
        """
        Read the next batch of the table's changes, with a dump of its rows or without.

        One call does one unit of work and hands back one frame; the caller loops for
        the next. Nothing is buffered and nothing is lazy, so the memory a batch costs
        is bounded by ``chunk_size`` however large the table is:

        ```python
        while (frame := dblog.fetch()) is not None:
            write(frame)
            dblog.commit()
        ```

        What it reads was settled at construction, so this takes no arguments: the
        table and the dump belong to the run, and repeating them per batch only invited
        the question of what a caller changing one halfway through meant.

        A dump batch is a chunk of rows plus the log window bracketing the read of it,
        as one frame: the window's events first, then the chunk minus whatever those
        events supersede. Once the table runs out, batches are windows alone. Without a
        dump every batch is a window.

        The first call seeds the run, inspecting the table and loading whatever progress
        is recorded; the rest just advance, so that metadata is read once for the run
        rather than once per batch.

        Returns:
            One frame of change events, of table rows, or of both, or None when there
            was nothing to read: the table is walked and the log is caught up as of
            this call. None is not permanent — a later call picks up whatever has
            landed since — so it ends a loop rather than the run. Frames carry the
            log's metadata columns ahead of the table's and all share one schema, so
            successive batches stack without reconciling. Rows that came off the table
            rather than out of the log are marked as such: their LSN columns are all
            zeros, a position the log never issues, which also orders them below every
            event.

        Raises:
            ValueError: If the table has no capture instance, or the dump last ran
                against a different table.
            CdcRetentionExpiredError: If the position to start from has aged out of
                the log.
        """

        if not self._started:
            self._start()
            self._started = True

        if self._dump is not None and not self._dump_done:
            # The window may only close once the log consumer has passed the high
            # watermark, or a row changed during the scan can be missing from it.
            low = self._connector.watermark()
            chunk = self._next_chunk()
            high = self._connector.watermark()
            self._connector.await_watermark(high)

            window = self._read_window()

            # A chunk that came back empty ends the dump and leaves the window to
            # stand as the batch on its own, if it held anything.
            if chunk is not None:
                return self._merge_chunk(chunk, window, low)

            return window

        return self._read_window()

    def _supersede(self, chunk: DataFrame, window: DataFrame | None) -> DataFrame:
        """
        Drop the chunk rows the window already carries.

        A row appearing in both was read from the table and changed in the log at
        around the same time, and there is no telling which image the chunk caught.
        The log's is the newer one by construction — it is a record of a change made
        to the row — so the chunk's copy is dropped and the event stands in for it.

        Rows are matched on the whole primary key, and the chunk keeps its order:
        chunks arrive sorted by key, and a consumer applying them in that order sees
        keys advance monotonically.

        Args:
            chunk: Rows read from the table.
            window: Events from the log window bracketing that read, or None.

        Returns:
            The chunk without the rows the window supersedes.

        Raises:
            RuntimeError: If the run state has not been seeded yet.
        """

        if self._spec is None:
            raise RuntimeError("run not started: no primary key to merge on")

        if window is None or window.is_empty():
            return chunk

        superseded = window.select(self._spec.pk_columns).unique()

        merged = chunk.join(
            superseded,
            on=self._spec.pk_columns,
            how="anti",
            maintain_order="left",
        )

        logger.debug(
            f"merged chunk: {chunk.height} rows, {chunk.height - merged.height} "
            f"superseded by the window, {merged.height} emitted"
        )

        return merged

    def _merge_chunk(
        self, chunk: DataFrame, window: DataFrame | None, low: datetime
    ) -> DataFrame:
        """
        Combine a chunk with the window bracketing it into one frame.

        Drops what the window supersedes from the chunk, stamps what is left into an
        event, then puts the window's own events ahead of it. The stamping is what
        makes this a single schema: the frame a chunk produces ends up with the same
        columns as the frame a window produces, so a consumer can stack every frame
        of the run without reconciling two layouts.

        Args:
            chunk: Rows read from the table.
            window: Events from the log window bracketing that read, or None.
            low: The watermark taken before the chunk was read; dates the emitted
                rows, none of which can be older than it.

        Returns:
            The window's events, if any, followed by the surviving chunk rows, all
            in the event schema.

        Raises:
            RuntimeError: If the run state has not been seeded yet.
        """

        if self._spec is None:
            raise RuntimeError("run not started: no primary key to merge on")

        events = self._connector.to_events(
            self._supersede(chunk, window), self._spec, low
        )

        if window is None or window.is_empty():
            return events

        return concat([window, events])

    def _next_chunk(self) -> DataFrame | None:
        """
        Read the next page of table rows and leave ``_chunk_key`` past its last row.

        Pages are sized by row count, not by key width: each one asks for
        ``chunk_size`` rows from ``_chunk_key`` onward and takes the last row's
        leading key as where the next page opens. A page is therefore the same size
        whatever the keys look like, which keeps both the memory it costs and the log
        window bracketing it bounded — key-width pages give neither on a table whose
        keys bunch up or leave gaps.

        The key advances one past the last row read, because ``read_table`` is
        inclusive on ``start_pk`` and reopening on that key would read its row twice.

        Fewer rows than asked for means the table had nothing more to give, and ends
        the dump.

        Returns:
            The rows in the page, or None once the dump is over.

        Raises:
            RuntimeError: If the run state has not been seeded yet.
            TypeError: If the leading key is not an integer, so it cannot be advanced.
            ValueError: If the leading key repeats inside the page.
        """

        if self._spec is None:
            raise RuntimeError("run not started: no table spec to read rows from")

        if self._dump_done:
            return None

        rows = self._connector.read_table(
            self._spec, start_pk=self._chunk_key, limit=self._chunk_size
        )

        if rows.height < self._chunk_size:
            self._dump_done = True

        if rows.is_empty():
            return None

        self._chunk_key = self._key_after(rows)

        logger.info(
            f"read chunk of {rows.height} rows from {self._spec.qualified_name}, "
            f"next chunk opens at {self._chunk_key}"
        )

        return rows

    def _key_after(self, rows: DataFrame) -> int:
        """
        Where the page following ``rows`` opens.

        Args:
            rows: A page of table rows, ordered by primary key.

        Returns:
            One past the leading key of the page's last row.

        Raises:
            TypeError: If the leading key is not an integer.
            ValueError: If the leading key repeats inside the page, which would make
                advancing past it drop rows.
        """

        assert self._spec is not None
        keys = rows[self._spec.pk_columns[0]]

        if keys.n_unique() != rows.height:
            raise ValueError(
                f"{self._spec.qualified_name} leading key "
                f"{self._spec.pk_columns[0]!r} is not unique within a chunk: rows "
                "sharing a key would be split across chunks and the ones past the "
                "boundary dropped"
            )

        last = keys[-1]
        # bool is an int as far as isinstance is concerned, and a bit column would
        # otherwise pass here and then produce a nonsense key.
        if isinstance(last, bool) or not isinstance(last, int):
            raise TypeError(
                f"{self._spec.qualified_name} leading key "
                f"{self._spec.pk_columns[0]!r} must be an integer to page by, got "
                f"{type(last).__name__}"
            )

        return last + 1

    def _read_window(self) -> DataFrame | None:
        """
        Read one window of the change log and leave ``_last_lsn`` past its end.

        The window runs from ``_last_lsn`` to whatever ``get_max_lsn`` reports at the
        moment it is asked, which is the log position the source has reached.
        ``_last_lsn`` then moves to one LSN beyond that high bound, because the CDC
        read is inclusive on both sides and reopening at the high bound would deliver
        every event sitting on it a second time.

        It advances even when the window turned up nothing, so a table with no writes
        does not re-scan an ever-widening range.

        Returns:
            The events in the window, or None when it held none. A ``_last_lsn``
            already past the source's max LSN is the steady state of an idle database
            and returns None without touching it.

        Raises:
            RuntimeError: If the run state has not been seeded yet.
        """

        if self._spec is None or self._last_lsn is None:
            raise RuntimeError("run not started: no table spec or LSN to read from")

        high = self._connector.get_max_lsn()
        if self._last_lsn > high:
            logger.debug(
                f"no window to read on {self._spec.qualified_name}: "
                f"{self._last_lsn.hex()} is already past the source's "
                f"{high.hex()}"
            )
            return None

        low = self._last_lsn
        events = self._connector.read_event_log(self._spec, low, high)
        self._last_lsn = self._connector.increment_lsn(high)

        logger.info(
            f"read window ({low.hex()}, {high.hex()}] on "
            f"{self._spec.qualified_name}: {0 if events is None else events.height} "
            "events"
        )
        logger.debug(f"next window opens at {self._last_lsn.hex()}")

        return events

    def connect(self) -> None:
        """Open the connection to the source."""

        self._connector.connect()

    def close(self) -> None:
        """Close the connection to the source."""

        self._connector.close()

    def __enter__(self) -> "DBLog":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
