"""
The DBLog algorithm, driven over a ``SourceConnector``.

``dblog()`` owns the algorithm; the connector owns the database. The connector is typed
as the Protocol rather than as any concrete class, so the algorithm can only ever reach
for the primitives every source must provide — a second source type needs no change
here.

The run is a value, not an object. One call reads one batch and hands back both the
frame and the state to carry on from, so nothing about where a run got to lives in this
module between calls. That is what lets a caller keep the position wherever the data
goes, and what makes the two decisions the library has no business making — where to
save state, and when to dump the table again — the caller's.
"""

import logging
from datetime import UTC, datetime, timedelta

from polars import DataFrame, concat

from pydblog.connectors.base import SourceConnector
from pydblog.connectors.types import LSN, TableSpec
from pydblog.state import BatchResult, RunState

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_INSPECT_EVERY = timedelta(days=1)

logger = logging.getLogger(__name__)


class CdcRetentionExpiredError(RuntimeError):
    """
    Raised when a run would read below what the change log still retains.

    Its own type because the recovery is specific: the gap cannot be filled from the
    log, so the caller has to dump the table again — ``state=None`` — rather than retry
    the read.
    """


class SchemaChangedError(RuntimeError):
    """
    Raised when the table's capture instance no longer projects what the run has been
    emitting.

    Every frame of a run shares one schema, and a consumer stacking them finds out
    otherwise at its own ``concat``, long after the batch that caused it was written.
    So a capture instance that changed under a run stops it here instead.

    Its own type for the same reason as ``CdcRetentionExpiredError``: the recovery is
    a fresh dump under the new spec, since nothing else produces a consistent stream
    again.
    """


def _require_capture_instance(spec: TableSpec) -> str:
    """
    The table's capture instance, refused if it has none.

    Raises:
        ValueError: If the table has no capture instance, so there is no change log to
            read and no floor to check a position against.
    """

    if spec.capture_instance is None:
        raise ValueError(
            f"{spec.qualified_name} has no capture instance: nothing to read a "
            "change log from"
        )

    return spec.capture_instance


def _seed(connector: SourceConnector, schema: str, table: str) -> RunState:
    """
    Open a fresh run on a table, at the present.

    A dump's chunks carry the table as it is now, so replaying the log's whole
    retention window would be work that changes nothing: the first window opens at the
    present instead. The floor matters in that choice — a capture instance enabled
    moments ago has a start LSN the database-wide max has not reached, and opening at
    the bare max would sit below what the log retains.

    Note that the CDC read is inclusive, so a run opening at the max LSN sees the event
    sitting exactly on it a second time. That is the safe direction: the merge drops it,
    whereas opening one LSN later would lose a write committed at that moment.

    Raises:
        ValueError: If the table has no capture instance to read a log from.
    """

    spec = connector.inspect(schema, table)
    capture_instance = _require_capture_instance(spec)
    floor = connector.get_min_lsn(capture_instance)
    opening = max(connector.get_max_lsn(), floor)

    # Loud on purpose. No state means "walk the whole table", so a caller whose state
    # load quietly returned None re-dumps everything — their bug, but an expensive
    # enough one to say out loud rather than log at info alongside the ordinary reads.
    logger.warning(
        f"no state given: starting a full dump of {spec.qualified_name} from "
        f"{opening.hex()}"
    )

    return RunState(spec=spec, last_lsn=opening, last_inspect=datetime.now(UTC))


def _check_table(state: RunState, schema: str, table: str) -> None:
    """
    Refuse a state that belongs to another table.

    The state's chunk key is a position in that table's key space and its LSN was read
    against that table's capture instance, so neither means anything here. Cheap, and
    the one thing that catches a caller loading the wrong saved state.

    Raises:
        ValueError: If the state describes a different table.
    """

    asked = f"{schema}.{table}"
    if state.spec.qualified_name != asked:
        raise ValueError(
            f"state belongs to {state.spec.qualified_name}, not {asked}: its chunk key "
            "is a position in a key space this table does not share"
        )


def _drift(before: list[str], after: list[str]) -> str:
    """A phrase naming what moved between two column lists, for an error or a log."""

    gained = sorted(set(after) - set(before))
    lost = sorted(set(before) - set(after))

    parts = []
    if gained:
        parts.append(f"gained {', '.join(gained)}")
    if lost:
        parts.append(f"lost {', '.join(lost)}")
    if not parts:
        # Same names, so what changed is a type. Which one is in the spec, and saying
        # so beats an empty phrase that reads like nothing happened.
        parts.append("changed the type of a column it still names the same")

    return " and ".join(parts)


def _refresh_spec(
    connector: SourceConnector, state: RunState, inspect_every: timedelta | None
) -> RunState:
    """
    Re-read the table's spec if the one in the state has gone stale.

    A run can now live for months on one state, so the spec inside it drifts from the
    table. Re-reading it per call would be a round trip per batch, so it happens on an
    interval instead — and ``inspect()`` validates the spec as it builds it, which is
    where a captured column that vanished from the source or whose type no longer reads
    back the same is caught.

    What this has to decide is whether the *frame schema* moved:

    - Captured columns or the primary key differ, and the capture instance changed under
      the run. Frames after this point would not stack with frames before it, so the run
      stops.
    - Only the source's own columns differ, and CDC still carries the set its capture
      instance was created with. Neither read projects the difference and no frame
      changes shape, so this is a warning: it is the case the interval exists to
      surface, otherwise invisible until someone re-creates the capture instance.

    Raises:
        ValueError: If the table lost its capture instance.
        SchemaChangedError: If the capture instance no longer projects what the run has
            been emitting.
    """

    if inspect_every is None:
        return state

    now = datetime.now(UTC)
    if now - state.last_inspect < inspect_every:
        return state

    fresh = connector.inspect(state.spec.source_schema, state.spec.source_table)
    _require_capture_instance(fresh)

    was, is_now = state.spec, fresh

    if was.pk_columns != is_now.pk_columns:
        raise SchemaChangedError(
            f"{is_now.qualified_name} is now keyed on "
            f"{', '.join(is_now.pk_columns)} rather than "
            f"{', '.join(was.pk_columns)}: the chunk key this run is paging by is a "
            "position in the old key space, and events can no longer be matched "
            "against chunk rows on the same columns. The table has to be dumped again."
        )

    if was.captured_columns != is_now.captured_columns:
        raise SchemaChangedError(
            f"the capture instance on {is_now.qualified_name} "
            f"{_drift(was.business_columns, is_now.business_columns)}: frames after "
            "this point would not stack with the ones this run has already emitted. "
            "The table has to be dumped again."
        )

    if was.columns != is_now.columns:
        logger.warning(
            f"{is_now.qualified_name} "
            f"{_drift([c.name for c in was.columns], [c.name for c in is_now.columns])}"
            ", but its capture instance still carries "
            f"{', '.join(is_now.business_columns)}. No frame changes shape, and the "
            "difference stays invisible to both reads until the capture instance is "
            "re-created."
        )

    logger.debug(f"re-inspected {is_now.qualified_name}")

    return state.model_copy(update={"spec": is_now, "last_inspect": now})


def _check_retention(
    connector: SourceConnector, state: RunState, capture_instance: str
) -> None:
    """
    Refuse a state whose position has aged out of the log.

    Checked on every call, not only when a run opens: a state persisted days ago is the
    ordinary input now, so a position below the floor is a thing that happens while
    nobody is looking rather than a mistake made at the start.

    Raises:
        CdcRetentionExpiredError: If the next window would open below the floor.
    """

    floor = connector.get_min_lsn(capture_instance)
    if state.last_lsn < floor:
        raise CdcRetentionExpiredError(
            f"{state.spec.qualified_name} no longer retains {state.last_lsn.hex()}: "
            f"the log starts at {floor.hex()}. The gap cannot be read, so the table "
            "has to be dumped again."
        )


def _supersede(chunk: DataFrame, window: DataFrame | None, spec: TableSpec) -> DataFrame:
    """
    Drop the chunk rows the window already carries.

    A row appearing in both was read from the table and changed in the log at around
    the same time, and there is no telling which image the chunk caught. The log's is
    the newer one by construction — it is a record of a change made to the row — so the
    chunk's copy is dropped and the event stands in for it.

    Rows are matched on the whole primary key, and the chunk keeps its order: chunks
    arrive sorted by key, and a consumer applying them in that order sees keys advance
    monotonically.

    The overlap is checked on the key columns before anything is rebuilt. A window that
    supersedes none of the chunk — the ordinary case, since a window covers only what
    changed while one chunk was read — then hands the chunk straight back untouched.
    The anti-join would return exactly the same rows, but it materialises every column
    to do it: measured on a 500,000-row chunk of 41 columns, that is 327 MB and 27ms to
    reproduce a frame we already had, against 0 MB and 2ms for the key-only check.

    Args:
        chunk: Rows read from the table.
        window: Events from the log window bracketing that read, or None.
        spec: The table, for the key columns to match on.

    Returns:
        The chunk without the rows the window supersedes, which is the chunk itself when
        it supersedes none of them.
    """

    if window is None or window.is_empty():
        return chunk

    keys = spec.pk_columns
    superseded = window.select(keys).unique()

    if chunk.select(keys).join(superseded, on=keys, how="semi").is_empty():
        logger.debug(
            f"merged chunk: {chunk.height} rows, none superseded by the window"
        )
        return chunk

    merged = chunk.join(superseded, on=keys, how="anti", maintain_order="left")

    logger.debug(
        f"merged chunk: {chunk.height} rows, {chunk.height - merged.height} "
        f"superseded by the window, {merged.height} emitted"
    )

    return merged


def _merge_chunk(
    connector: SourceConnector,
    chunk: DataFrame,
    window: DataFrame | None,
    low: datetime,
    spec: TableSpec,
) -> DataFrame:
    """
    Combine a chunk with the window bracketing it into one frame.

    Drops what the window supersedes from the chunk, stamps what is left into an event,
    then puts the window's own events ahead of it. The stamping is what makes this a
    single schema: the frame a chunk produces ends up with the same columns as the frame
    a window produces, so a consumer can stack every frame of the run without
    reconciling two layouts.

    Args:
        connector: The source, for the stamping.
        chunk: Rows read from the table.
        window: Events from the log window bracketing that read, or None.
        low: The watermark taken before the chunk was read; dates the emitted rows, none
            of which can be older than it.
        spec: The table being read.

    Returns:
        The window's events, if any, followed by the surviving chunk rows, all in the
        event schema.
    """

    events = connector.to_events(_supersede(chunk, window, spec), spec, low)

    if window is None or window.is_empty():
        return events

    return concat([window, events])


def _key_after(rows: DataFrame, spec: TableSpec) -> int:
    """
    Where the page following ``rows`` opens.

    Args:
        rows: A page of table rows, ordered by primary key.
        spec: The table, for the leading key column.

    Returns:
        One past the leading key of the page's last row.

    Raises:
        TypeError: If the leading key is not an integer.
        ValueError: If the leading key repeats inside the page, which would make
            advancing past it drop rows.
    """

    leading = spec.pk_columns[0]
    keys = rows[leading]

    if keys.n_unique() != rows.height:
        raise ValueError(
            f"{spec.qualified_name} leading key {leading!r} is not unique within a "
            "chunk: rows sharing a key would be split across chunks and the ones past "
            "the boundary dropped"
        )

    last = keys[-1]
    # bool is an int as far as isinstance is concerned, and a bit column would
    # otherwise pass here and then produce a nonsense key.
    if isinstance(last, bool) or not isinstance(last, int):
        raise TypeError(
            f"{spec.qualified_name} leading key {leading!r} must be an integer to page "
            f"by, got {type(last).__name__}"
        )

    return last + 1


def _next_chunk(
    connector: SourceConnector, state: RunState, chunk_size: int
) -> tuple[DataFrame | None, RunState]:
    """
    Read the next page of table rows and advance the state past its last row.

    Pages are sized by row count, not by key width: each one asks for ``chunk_size``
    rows from ``chunk_key`` onward and takes the last row's leading key as where the
    next page opens. A page is therefore the same size whatever the keys look like,
    which keeps both the memory it costs and the log window bracketing it bounded —
    key-width pages give neither on a table whose keys bunch up or leave gaps.

    The key advances one past the last row read, because ``read_table`` is inclusive on
    ``start_pk`` and reopening on that key would read its row twice.

    Fewer rows than asked for means the table had nothing more to give, and ends the
    dump. That is a comparison against the size *this* call asked for, so a caller who
    changes ``chunk_size`` between calls is safe: a short page is short either way.

    Returns:
        The rows in the page and the state advanced past them, or None and the state
        marked done once the table runs out.

    Raises:
        TypeError: If the leading key is not an integer, so it cannot be advanced.
        ValueError: If the leading key repeats inside the page.
    """

    rows = connector.read_table(
        state.spec, start_pk=state.chunk_key, limit=chunk_size
    )

    done = rows.height < chunk_size

    if rows.is_empty():
        return None, state.model_copy(update={"dump_done": done})

    chunk_key = _key_after(rows, state.spec)

    logger.info(
        f"read chunk of {rows.height} rows from {state.spec.qualified_name}, "
        f"next chunk opens at {chunk_key}"
    )

    return rows, state.model_copy(
        update={"chunk_key": chunk_key, "dump_done": done}
    )


def _read_window(
    connector: SourceConnector, state: RunState
) -> tuple[DataFrame | None, RunState]:
    """
    Read one window of the change log and advance the state past its end.

    The window runs from ``last_lsn`` to whatever ``get_max_lsn`` reports at the moment
    it is asked, which is the log position the source has reached. ``last_lsn`` then
    moves to one LSN beyond that high bound, because the CDC read is inclusive on both
    sides and reopening at the high bound would deliver every event sitting on it a
    second time.

    It advances even when the window turned up nothing, so a table with no writes does
    not re-scan an ever-widening range — which is why the caller has to keep the state
    off a result whose frame is None.

    Returns:
        The events in the window and the advanced state, or None and the state
        untouched. A ``last_lsn`` already past the source's max LSN is the steady state
        of an idle database and reads nothing.
    """

    high = connector.get_max_lsn()
    if state.last_lsn > high:
        logger.debug(
            f"no window to read on {state.spec.qualified_name}: "
            f"{state.last_lsn.hex()} is already past the source's {high.hex()}"
        )
        return None, state

    low = state.last_lsn
    events = connector.read_event_log(state.spec, low, high)
    next_lsn: LSN = connector.increment_lsn(high)

    logger.info(
        f"read window ({low.hex()}, {high.hex()}] on {state.spec.qualified_name}: "
        f"{0 if events is None else events.height} events"
    )
    logger.debug(f"next window opens at {next_lsn.hex()}")

    return events, state.model_copy(update={"last_lsn": next_lsn})


def dblog(
    connector: SourceConnector,
    schema: str,
    table: str,
    state: RunState | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    inspect_every: timedelta | None = DEFAULT_INSPECT_EVERY,
) -> BatchResult:
    """
    Read one batch of a table's changes, with a chunk of its rows or without.

    One call does one unit of work and hands back one frame plus the state to carry on
    from; the caller loops for the next. Nothing is buffered and nothing is lazy, so the
    memory a batch costs is bounded by ``chunk_size`` however large the table is:

    ```python
    with build_connector(...) as source:
        state = load_my_state()          # None the first time
        while True:
            try:
                result = dblog(source, "dbo", "sales", state=state)
            except (CdcRetentionExpiredError, SchemaChangedError):
                state = None             # dump the table again
                continue

            if result.frame is not None:
                write(result.frame)
            save_my_state(result.state)  # ideally alongside that write
            state = result.state

            if result.frame is None:
                break
    ```

    The state controls everything. With none, the run opens at the present and dumps the
    table. With one, the batch carries on from where that state left off. With one whose
    ``dump_done`` is set — which is what a dump becomes once the table is walked — the
    batch is a window of the log alone, so the same call slides from dumping into
    tailing with nothing retrieved from anywhere.

    A dump batch is a chunk of rows plus the log window bracketing the read of it, as
    one frame: the window's events first, then the chunk minus whatever those events
    supersede. The window closes after the chunk was scanned and only once the log
    consumer has passed a watermark taken after it, or a row changed during the scan
    could be missing from it.

    Nothing is written anywhere. The position comes back instead, so it cannot get ahead
    of the caller's own write: a frame received but never saved is read again by the next
    call given the older state, which is the safe direction to fail in and the
    at-least-once guarantee the algorithm already makes.

    Args:
        connector: The source to read from, already connected.
        schema: Schema of the table to read.
        table: Name of the table to read. With a state, this and ``schema`` are checked
            against it rather than used, which is what catches the wrong state being
            loaded.
        state: Where to carry on from, or None to open a fresh run and dump the table.
        chunk_size: Rows per dump chunk. Bounds both memory per chunk and the width of
            the log window bracketing it. Free to differ between calls: pages are read
            by key, so there is no plan for a new size to invalidate.
        inspect_every: How stale the state's table spec may get before it is re-read.
            None never re-reads it, which pins the run to the table as it was when the
            run opened.

    Returns:
        The batch and the state to carry on from. The frame is None when there was
        nothing to read: the table is walked and the log is caught up as of this call.
        That ends a loop rather than the run — a later call picks up whatever has landed
        since — and the state still has to be kept, since an empty window advances it.
        Frames carry the log's metadata columns ahead of the table's and all share one
        schema, so successive batches stack without reconciling. Rows that came off the
        table rather than out of the log are marked as such: their LSN columns are all
        zeros, a position the log never issues, which also orders them below every event.

    Raises:
        ValueError: If ``chunk_size`` is below 1, if the table has no capture instance,
            or if the state belongs to another table.
        CdcRetentionExpiredError: If the state's position has aged out of the log.
        SchemaChangedError: If the table's capture instance no longer projects what the
            run has been emitting.
    """

    if chunk_size < 1:
        raise ValueError(f"chunk_size must be at least 1, got {chunk_size}")

    if state is None:
        state = _seed(connector, schema, table)
    else:
        _check_table(state, schema, table)
        state = _refresh_spec(connector, state, inspect_every)
        _check_retention(connector, state, _require_capture_instance(state.spec))

    if state.dump_done:
        window, state = _read_window(connector, state)
        return BatchResult(frame=window, state=state)

    # The window may only close once the log consumer has passed the high watermark, or
    # a row changed during the scan can be missing from it.
    low = connector.watermark()
    chunk, state = _next_chunk(connector, state, chunk_size)
    high = connector.watermark()
    connector.await_watermark(high)

    window, state = _read_window(connector, state)

    # A chunk that came back empty ends the dump and leaves the window to stand as the
    # batch on its own, if it held anything.
    if chunk is None:
        return BatchResult(frame=window, state=state)

    return BatchResult(
        frame=_merge_chunk(connector, chunk, window, low, state.spec), state=state
    )
