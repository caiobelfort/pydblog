"""
The DBLog algorithm, driven over a ``SourceConnector``.

``DBLog`` owns the algorithm; the connector owns the database. The attribute holding
the connector is typed as the Protocol rather than as any concrete class, so the
algorithm can only ever reach for the ten primitives every source must provide — a
second source type needs no change here.
"""

from collections.abc import Iterator
from types import TracebackType

import logfire
from polars import DataFrame

from pydblog.connectors.base import SourceConnector, build_connector
from pydblog.connectors.types import LSN, SourceType, TableSpec
from pydblog.state import DumpState, JsonFileStore, StateStore

DEFAULT_CHUNK_SIZE = 1000


class CdcRetentionExpiredError(RuntimeError):
    """
    Raised when a run is asked to start below what the change log still retains.

    Its own type because the recovery is specific: the gap cannot be filled from the
    log, so the caller has to start a fresh dump rather than retry the read.
    """


class DBLog:
    """
    Interleaves a chunked table dump with the ongoing change event log.

    The dump walks the table in chunks of ``chunk_size`` rows, keyed off the leading
    primary key column. Around each chunk sits a window of log events; events in that
    window win over the chunk's rows, since they are the newer image. The result is a
    single stream where every row is delivered at least once and never stale.

    Run state lives on the instance and only in memory: a run that dies restarts from
    the beginning.
    """

    def __init__(
        self,
        source_type: SourceType | str,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        state_store: StateStore | None = None,
        **kwargs,
    ) -> None:
        """
        Build the connector for the source and set up an empty run state.

        Args:
            source_type: Which source to talk to; selects the connector.
            host: Source hostname.
            port: Source port.
            user: Username to authenticate with.
            password: Password to authenticate with.
            database: Database to read from.
            chunk_size: Rows per dump chunk. Bounds both memory per chunk and the
                width of the log window bracketing it.
            state_store: Where dump progress is kept, so an interrupted dump can be
                picked up. Defaults to JSON files in the working directory.
            **kwargs: Passed through to the connector unchanged.

        Raises:
            ValueError: If ``chunk_size`` is not at least 1.
        """

        if chunk_size < 1:
            raise ValueError(f"chunk_size must be at least 1, got {chunk_size}")

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

        # Run state. All of it is filled in when a run starts and mutated as it goes.
        self._spec: TableSpec | None = None  # table being dumped, from inspect()
        self._dump: str | None = None  # identity progress is recorded under
        self._last_lsn: LSN | None = None  # log position reached; next window opens here
        self._chunk_key: int | None = None  # leading PK the next chunk starts at
        self._dump_done: bool = False  # set once a chunk comes back short

    @property
    def last_lsn(self) -> LSN | None:
        """
        The log position the run has reached, or None before it starts.

        This is the handoff: passing it back as ``from_lsn`` on a later run picks up
        exactly where this one stopped, with nothing repeated and nothing skipped.
        """

        return self._last_lsn

    def _start(
        self, schema: str, table: str, dump: str | None, from_lsn: LSN | None
    ) -> None:
        """
        Seed the run state for a table, picking up recorded progress if there is any.

        A named dump that has run before starts from what it recorded rather than
        from anywhere the caller suggests: its chunk key and its LSN were written
        together and only mean anything together, so honouring a ``from_lsn`` against
        a resumed chunk key would leave a gap between them.

        Otherwise, without a ``from_lsn``, the run opens at the retention floor and
        reads every event the log still holds. Asking for no particular starting
        point means asking to skip nothing, and the floor is the earliest there is
        anything to skip from.

        Args:
            schema: Schema of the table to read.
            table: Name of the table to read.
            dump: Identity of the dump to run or resume, or None to skip the dump.
            from_lsn: Where to open the first window, or None to read every event the
                log still holds.

        Raises:
            ValueError: If the table has no capture instance to read a log from, or
                the dump last ran against a different table.
            CdcRetentionExpiredError: If the position to start from is below the
                retention floor.
        """

        spec = self._connector.inspect(schema, table)
        if spec.capture_instance is None:
            raise ValueError(
                f"{spec.qualified_name} has no capture instance: nothing to read a "
                "change log from"
            )

        recorded = self._store.load(dump) if dump is not None else None
        if recorded is not None and recorded.table != spec.qualified_name:
            raise ValueError(
                f"dump {dump!r} last ran against {recorded.table}, not "
                f"{spec.qualified_name}: its chunk key is a position in a key space "
                "this table does not share"
            )

        floor = self._connector.get_min_lsn(spec.capture_instance)
        if recorded is not None:
            start = recorded.last_lsn
        elif from_lsn is not None:
            start = from_lsn
        else:
            start = floor

        if start < floor:
            raise CdcRetentionExpiredError(
                f"{spec.qualified_name} no longer retains {start.hex()}: the log "
                f"starts at {floor.hex()}. The gap cannot be read, so the table has "
                "to be dumped again."
            )

        self._spec = spec
        self._dump = dump
        self._last_lsn = start
        self._chunk_key = recorded.chunk_key if recorded is not None else None
        self._dump_done = recorded.done if recorded is not None else False

        logfire.info(
            "Started run",
            table=spec.qualified_name,
            dump=dump,
            resumed=recorded is not None,
            from_lsn=start.hex(),
            chunk_key=self._chunk_key,
        )

    def _checkpoint(self) -> None:
        """
        Record how far the dump has got, so an interruption costs one chunk.

        The table position and the log position go down together: a resume needs both
        to agree, and either one on its own would leave rows that neither the
        remaining chunks nor the remaining log would deliver.

        Does nothing for a run with no dump to record against.
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

    def run(
        self,
        schema: str,
        table: str,
        dump: str | None = None,
        from_lsn: LSN | None = None,
    ) -> Iterator[DataFrame]:
        """
        Stream a table's changes, optionally interleaved with a dump of its rows.

        Naming a ``dump`` makes the run also walk the table in chunks, so a consumer
        starting from nothing ends up with every row. The name is the dump's identity,
        not a label: progress is recorded against it, so re-running under the same
        name carries on where that dump stopped, and a new name starts over. Leaving
        it unnamed drains the change log only, which is what a consumer that already
        holds the table wants.

        A dump run alternates the two: a chunk of rows, then the log window that
        brackets reading it, with the window's events emitted first and the chunk
        emitted after, minus whatever the window supersedes. It ends when the table
        runs out. Progress is recorded after each chunk, so a run cut short by a lost
        connection resumes at the chunk it was on rather than at the start of the
        table; re-running the same name once the table is done skips the chunks and
        tails the log instead.

        A run with no dump drains the log and stops once it is caught up, rather than
        polling for more. A caller that wants to tail continuously loops over ``run``
        itself, handing ``last_lsn`` back in as ``from_lsn`` each time.

        This is a generator: nothing is read, and none of the errors below are
        raised, until it is iterated.

        Args:
            schema: Schema of the table to read.
            table: Name of the table to read.
            dump: Identity of the dump to run or resume, or None to skip the dump.
            from_lsn: Where to open the first window, or None to read every event the
                log still holds. Ignored when the dump has progress recorded.

        Yields:
            Frames of change events in log order, and, on a dump run, frames of table
            rows. The two have different shapes: events carry the log's metadata
            columns ahead of the table's.

        Raises:
            ValueError: If the table has no capture instance, or ``dump`` is blank.
            CdcRetentionExpiredError: If ``from_lsn`` has aged out of the log.
        """

        if dump is not None and not dump.strip():
            raise ValueError("dump name is blank: it is the key progress is kept under")

        self._start(schema, table, dump, from_lsn)

        if dump is not None and not self._dump_done:
            while not self._dump_done:
                # Order is the algorithm. _last_lsn already sits where the previous
                # window closed, which is a position the log had reached before this
                # scan began; the window then closes at the max LSN once the scan is
                # done. Anything committed while the chunk was being read therefore
                # falls inside the window, and the chunk cannot carry a stale row the
                # window does not also correct.
                chunk = self._next_chunk()
                window = self._read_window()

                if window is not None:
                    yield window

                if chunk is not None:
                    merged = self._merge_chunk(chunk, window)
                    if not merged.is_empty():
                        yield merged

                # After the yields, not before: this line only runs once the consumer
                # comes back for more, which is the only evidence there is that it
                # took what the iteration produced. Recording first would let a crash
                # skip a chunk nobody ever received.
                self._checkpoint()

            return

        while (events := self._read_window()) is not None:
            yield events
            self._checkpoint()

    def _merge_chunk(self, chunk: DataFrame, window: DataFrame | None) -> DataFrame:
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

        return chunk.join(
            superseded,
            on=self._spec.pk_columns,
            how="anti",
            maintain_order="left",
        )

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

        logfire.info(
            "Read chunk",
            table=self._spec.qualified_name,
            dump=self._dump,
            rows=rows.height,
            next_chunk_key=self._chunk_key,
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
            return None

        events = self._connector.read_event_log(self._spec, self._last_lsn, high)
        self._last_lsn = self._connector.increment_lsn(high)

        logfire.info(
            "Read log window",
            table=self._spec.qualified_name,
            events=0 if events is None else events.height,
        )

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
