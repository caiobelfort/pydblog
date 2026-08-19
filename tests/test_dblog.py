"""
DBLog algorithm tests.

None of this needs a database. ``dblog()`` only ever talks to the primitives of the
``SourceConnector`` Protocol, so a stub implementing that Protocol is enough to assert
the orchestration — which is the point of typing the argument as the Protocol rather
than as ``MSSQLConnector``.

Whether those primitives actually work against SQL Server is ``test_mssql.py``'s job,
and it is already answered there.
"""

from datetime import UTC, datetime, timedelta

import pytest
from polars import Binary, DataFrame, Datetime, Int32, all, lit

from pydblog.connectors.types import LSN, ColumnSpec, TableSpec
from pydblog.dblog import (
    CdcRetentionExpiredError,
    SchemaChangedError,
    _next_chunk,
    _read_window,
    _supersede,
    dblog,
)
from pydblog.state import RunState

# The table every test reads, named the way a caller names it.
TARGET = {"schema": "dbo", "table": "sales"}

COLUMNS = [
    ColumnSpec(name="sale_id", type_name="int", precision=10, scale=0),
    ColumnSpec(name="amount", type_name="decimal", precision=10, scale=2),
]

SPEC = TableSpec(
    source_schema="dbo",
    source_table="sales",
    pk_columns=["sale_id"],
    columns=COLUMNS,
    captured_columns=COLUMNS,
    capture_instance="dbo_sales",
)

# A column added to the source. CDC keeps the set its capture instance was created
# with, so which of the two lists it lands in is the whole difference between a warning
# and a stopped run.
WIDER = COLUMNS + [ColumnSpec(name="note", type_name="varchar", precision=0, scale=0)]

SPEC_SOURCE_WIDENED = SPEC.model_copy(update={"columns": WIDER})
SPEC_CAPTURE_WIDENED = SPEC.model_copy(
    update={"columns": WIDER, "captured_columns": WIDER}
)
SPEC_REKEYED = SPEC.model_copy(update={"pk_columns": ["amount"]})

COMMIT_TIME = datetime(2026, 8, 13, 12, 30)


def lsn(value: int) -> LSN:
    """An LSN is ten bytes, big-endian, so ordering by value is ordering by bytes."""
    return value.to_bytes(10, "big")


class StubConnector:
    """
    A scriptable ``SourceConnector`` that records what the algorithm asked it.

    ``max_lsn`` and ``events`` are what the next call returns; tests set them to stage
    a scenario. The ``*_calls`` lists are what the algorithm actually asked for, which
    is what the assertions are about.
    """

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.connects = 0
        self.closes = 0
        self.max_lsn: LSN = lsn(100)
        self.min_lsn: LSN = lsn(1)
        self.spec: TableSpec | None = None
        self.events: DataFrame | None = None
        # Scripts, for when one call is not enough: each drains one entry per call and
        # the source stops moving once it runs dry.
        self.max_lsn_script: list[LSN] = []
        self.event_script: list[DataFrame | None] | None = None
        self.spec_script: list[TableSpec] | None = None
        self.rows: DataFrame = DataFrame()
        self.row_script: list[DataFrame] | None = None
        self.event_log_calls: list[tuple[TableSpec, LSN, LSN]] = []
        self.read_table_calls: list[tuple[TableSpec, int | None, int | None, int]] = []
        self.inspect_calls: list[tuple[str, str]] = []
        self.watermarks: list[datetime] = []
        self.awaited: list[datetime] = []
        self.to_events_calls: list[tuple[DataFrame, datetime | None]] = []
        # Every read in the order it happened. The interleave is as much about sequence
        # as about results: the window has to close after the chunk scan.
        self.calls: list[str] = []

    def connect(self) -> None:
        self.connects += 1

    def close(self) -> None:
        self.closes += 1

    def __enter__(self) -> "StubConnector":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def get_max_lsn(self) -> LSN:
        if self.max_lsn_script:
            self.max_lsn = self.max_lsn_script.pop(0)
        return self.max_lsn

    def get_min_lsn(self, capture_instance: str) -> LSN:
        return self.min_lsn

    def inspect(self, schema: str, table: str) -> TableSpec:
        self.inspect_calls.append((schema, table))
        if self.spec_script:
            return self.spec_script.pop(0)
        return self.spec if self.spec is not None else SPEC

    def read_event_log(
        self, spec: TableSpec, from_lsn: LSN, to_lsn: LSN
    ) -> DataFrame | None:
        self.event_log_calls.append((spec, from_lsn, to_lsn))
        self.calls.append("read_event_log")
        if self.event_script is not None:
            return self.event_script.pop(0) if self.event_script else None
        return self.events

    def read_table(
        self,
        spec: TableSpec,
        start_pk: int | None = None,
        end_pk: int | None = None,
        limit: int = 0,
    ) -> DataFrame:
        self.read_table_calls.append((spec, start_pk, end_pk, limit))
        self.calls.append("read_table")
        if self.row_script is not None:
            return self.row_script.pop(0) if self.row_script else DataFrame()
        return self.rows

    def read_pk_range(self, spec: TableSpec) -> tuple[int, int] | None:
        return None

    def watermark(self) -> datetime:
        """A distinct, increasing value per call, so tests can tell them apart.

        Deliberately offset from ``COMMIT_TIME``: a stamp that happened to equal it
        would let a test pass without the watermark having reached the frame at all.
        """
        self.calls.append("watermark")
        mark = COMMIT_TIME + timedelta(seconds=1 + len(self.watermarks))
        self.watermarks.append(mark)
        return mark

    def await_watermark(self, mark: datetime) -> None:
        self.calls.append("await_watermark")
        self.awaited.append(mark)

    def to_events(
        self, rows: DataFrame, spec: TableSpec, commit_timestamp: datetime | None
    ) -> DataFrame:
        """Stamps the way MSSQLConnector does, so the merge assertions see the shape."""
        self.to_events_calls.append((rows, commit_timestamp))
        return rows.select(
            lit(bytes(10), Binary).alias("start_lsn"),
            lit(0, Int32).alias("operation"),
            lit(commit_timestamp, Datetime("us")).alias("commit_timestamp"),
            all(),
        )

    def increment_lsn(self, value: LSN) -> LSN:
        return lsn(int.from_bytes(value, "big") + 1)


@pytest.fixture
def source() -> StubConnector:
    """A stub source. Every test drives the algorithm against one of these."""
    return StubConnector()


def state(**overrides) -> RunState:
    """
    A state mid-run on dbo.sales, with the log read up to lsn(10).

    ``last_inspect`` defaults to now, so an ordinary call does not re-inspect and the
    call counts a test asserts on stay about what it drove. Tests about re-inspecting
    pass an older one.
    """
    fields = {
        "spec": SPEC,
        "last_lsn": lsn(10),
        "last_inspect": datetime.now(UTC),
    }
    return RunState(**{**fields, **overrides})


def sales(*sale_ids: int) -> DataFrame:
    """A chunk of table rows, keyed the way SPEC says."""
    return DataFrame({"sale_id": list(sale_ids), "amount": [1] * len(sale_ids)})


def events(*sale_ids: int) -> DataFrame:
    """A window of change events, shaped the way read_event_log returns them."""
    return DataFrame(
        {
            "start_lsn": [lsn(n) for n in sale_ids],
            "operation": [4] * len(sale_ids),
            "commit_timestamp": [COMMIT_TIME] * len(sale_ids),
            "sale_id": list(sale_ids),
            "amount": [99] * len(sale_ids),
        },
        schema_overrides={
            "start_lsn": Binary,
            "operation": Int32,
            "commit_timestamp": Datetime("us"),
        },
    )


def full_run(
    source: StubConnector, start: RunState | None = None, **kwargs
) -> tuple[list[DataFrame], RunState]:
    """
    Every batch a run has to give, the way a caller's loop collects them.

    Threads the state the way a caller must, including off the result whose frame is
    None: that one still advanced the log position.
    """
    frames: list[DataFrame] = []
    carried = start
    while True:
        result = dblog(source, **TARGET, state=carried, **kwargs)
        carried = result.state
        if result.frame is None:
            return frames, carried
        frames.append(result.frame)


def dump_only(
    source: StubConnector, start: RunState | None = None, **kwargs
) -> tuple[list[DataFrame], RunState]:
    """The batches up to the end of the table, the way a backfill bounds its loop."""
    frames: list[DataFrame] = []
    carried = start
    while carried is None or not carried.dump_done:
        result = dblog(source, **TARGET, state=carried, **kwargs)
        carried = result.state
        if result.frame is not None:
            frames.append(result.frame)
    return frames, carried


# ---------------------------------------------------------------------------
# Arguments — what is refused without asking the source
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chunk_size", [0, -1, -1000])
def test_rejects_a_non_positive_chunk_size(source, chunk_size):
    with pytest.raises(ValueError, match="chunk_size"):
        dblog(source, **TARGET, chunk_size=chunk_size)

    assert source.calls == []  # refused before a single round trip


def test_refuses_a_state_that_belongs_to_another_table(source):
    """
    Its chunk key is a position in that table's key space and its LSN was read against
    that table's capture instance, so neither means anything here. The cheapest thing
    that catches the wrong saved state being loaded.
    """
    with pytest.raises(ValueError, match="dbo.sales"):
        dblog(source, schema="dbo", table="refunds", state=state())


def test_the_wrong_table_is_caught_before_anything_is_read(source):
    with pytest.raises(ValueError):
        dblog(source, schema="dbo", table="refunds", state=state())

    assert source.calls == []


def test_refuses_a_table_that_is_not_captured(source):
    """Nothing to read a change log from, and nothing to check a position against."""
    source.spec = SPEC.model_copy(update={"capture_instance": None})

    with pytest.raises(ValueError, match="capture instance"):
        dblog(source, **TARGET)


# ---------------------------------------------------------------------------
# _read_window — one pass over the log, from the last LSN to the current max LSN
# ---------------------------------------------------------------------------


def test_reads_from_the_last_lsn_up_to_the_current_max_lsn(source):
    source.max_lsn = lsn(100)

    _read_window(source, state(last_lsn=lsn(10)))

    assert source.event_log_calls == [(SPEC, lsn(10), lsn(100))]


def test_returns_the_events_the_connector_read(source):
    source.events = DataFrame({"sale_id": [1, 2]})

    window, _ = _read_window(source, state())

    assert window is not None
    assert window.to_dicts() == [{"sale_id": 1}, {"sale_id": 2}]


def test_advances_the_last_lsn_one_past_the_window(source):
    """The CDC read is inclusive on both bounds, so reopening at the high LSN would
    deliver every event sitting on the boundary a second time."""
    source.max_lsn = lsn(100)

    _, advanced = _read_window(source, state(last_lsn=lsn(10)))

    assert advanced.last_lsn == lsn(101)


def test_reads_the_window_whose_bounds_meet(source):
    """A last LSN exactly at the max LSN is a one-LSN window, not an empty one."""
    source.max_lsn = lsn(100)

    _, advanced = _read_window(source, state(last_lsn=lsn(100)))

    assert source.event_log_calls == [(SPEC, lsn(100), lsn(100))]
    assert advanced.last_lsn == lsn(101)


def test_returns_none_when_the_window_holds_no_events(source):
    source.events = None

    window, _ = _read_window(source, state())

    assert window is None


def test_advances_the_last_lsn_even_when_the_window_was_empty(source):
    """Otherwise a quiet table re-scans the same widening range forever."""
    source.max_lsn = lsn(100)
    source.events = None

    _, advanced = _read_window(source, state(last_lsn=lsn(10)))

    assert advanced.last_lsn == lsn(101)


def test_does_not_query_when_the_last_lsn_is_past_the_max_lsn(source):
    """The steady state of a database with no new commits: it must cost nothing."""
    source.max_lsn = lsn(100)

    window, _ = _read_window(source, state(last_lsn=lsn(101)))

    assert window is None
    assert source.event_log_calls == []


def test_leaves_the_last_lsn_alone_when_there_is_nothing_new(source):
    source.max_lsn = lsn(100)
    carried = state(last_lsn=lsn(101))

    _, advanced = _read_window(source, carried)

    assert advanced.last_lsn == lsn(101)


def test_reading_a_window_leaves_the_state_it_was_given_alone(source):
    """The state is threaded, not mutated: the caller's copy has to stay put."""
    source.max_lsn = lsn(100)
    carried = state(last_lsn=lsn(10))

    _read_window(source, carried)

    assert carried.last_lsn == lsn(10)


# ---------------------------------------------------------------------------
# Seeding — where a run with no state starts reading from
# ---------------------------------------------------------------------------


def test_inspects_the_table_it_was_asked_for(source):
    dblog(source, **TARGET)

    assert source.inspect_calls == [("dbo", "sales")]


def test_a_state_carries_the_spec_so_later_calls_do_not_inspect(source):
    """A round trip per batch is what carrying the spec in the state is for."""
    source.row_script = [sales(1, 2), sales(3)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    full_run(source, chunk_size=2)

    assert source.inspect_calls == [("dbo", "sales")]


def test_a_starting_run_opens_at_the_present(source):
    """
    Its chunks carry the table as it is now, so replaying the log's whole retention
    window would be work that changes nothing.
    """
    source.max_lsn = lsn(500)
    source.min_lsn = lsn(1)

    result = dblog(source, **TARGET, chunk_size=2)

    assert source.event_log_calls[0][1] == lsn(500)
    assert result.state.spec == SPEC


def test_a_starting_run_falls_back_to_the_floor_above_the_max_lsn(source):
    """
    A capture instance enabled moments ago has a start LSN the database-wide max has
    not reached, and opening at the bare max would sit below what the log retains.
    """
    source.max_lsn = lsn(50)
    source.min_lsn = lsn(400)

    result = dblog(source, **TARGET, chunk_size=2)

    # Above the source's max, so there is no window to read yet and the position stays
    # where it opened — which is the point: it is above the floor rather than below it.
    assert result.state.last_lsn == lsn(400)
    assert source.event_log_calls == []


def test_a_starting_run_has_walked_none_of_the_table(source):
    source.rows = sales(1, 2, 3)

    result = dblog(source, **TARGET, chunk_size=3)

    assert result.state.chunk_key == 4
    assert result.state.dump_done is False
    assert source.read_table_calls[0][1] is None  # from the start of the table


def test_a_starting_run_stamps_when_it_inspected(source):
    before = datetime.now(UTC)

    result = dblog(source, **TARGET)

    assert before <= result.state.last_inspect <= datetime.now(UTC)


def test_starting_a_run_says_so_out_loud(source, caplog):
    """
    No state means "walk the whole table". A caller whose state load quietly returned
    None re-dumps everything, which is expensive enough to warn about rather than
    mention at info alongside the ordinary reads.
    """
    with caplog.at_level("WARNING"):
        dblog(source, **TARGET)

    assert "full dump of dbo.sales" in caplog.text


def test_carrying_a_state_says_nothing_of_the_kind(source, caplog):
    with caplog.at_level("WARNING"):
        dblog(source, **TARGET, state=state())

    assert caplog.text == ""


# ---------------------------------------------------------------------------
# _next_chunk — one keyset page of the table, sized by row count
# ---------------------------------------------------------------------------


def test_first_chunk_reads_from_the_start_of_the_table(source):
    source.rows = sales(1, 2, 3)

    _next_chunk(source, state(), 3)

    assert source.read_table_calls == [(SPEC, None, None, 3)]


def test_later_chunks_read_from_where_the_last_one_stopped(source):
    source.rows = sales(7, 8, 9)

    _next_chunk(source, state(chunk_key=7), 3)

    assert source.read_table_calls == [(SPEC, 7, None, 3)]


def test_returns_the_rows_it_read(source):
    source.rows = sales(1, 2, 3)

    rows, _ = _next_chunk(source, state(), 3)

    assert rows is not None
    assert rows["sale_id"].to_list() == [1, 2, 3]


def test_advances_past_the_last_row_of_the_chunk(source):
    """``read_table`` is inclusive on ``start_pk``, so reopening on that key would read
    its row twice."""
    source.rows = sales(1, 2, 3)

    _, advanced = _next_chunk(source, state(), 3)

    assert advanced.chunk_key == 4


def test_a_full_chunk_leaves_the_dump_running(source):
    source.rows = sales(1, 2, 3)

    _, advanced = _next_chunk(source, state(), 3)

    assert advanced.dump_done is False


def test_a_short_chunk_ends_the_dump(source):
    """Fewer rows than asked for means the table had nothing more to give."""
    source.rows = sales(1, 2)

    rows, advanced = _next_chunk(source, state(), 3)

    assert rows is not None
    assert advanced.dump_done is True


def test_an_empty_chunk_ends_the_dump_and_yields_nothing(source):
    source.rows = sales()

    rows, advanced = _next_chunk(source, state(), 3)

    assert rows is None
    assert advanced.dump_done is True


def test_an_empty_chunk_does_not_move_the_chunk_key(source):
    source.rows = sales()

    _, advanced = _next_chunk(source, state(chunk_key=7), 3)

    assert advanced.chunk_key == 7


def test_a_page_is_short_against_the_size_this_call_asked_for(source):
    """
    ``chunk_size`` may differ between calls, since pages are read by key and there is
    no plan for a new size to invalidate. What must not happen is a full page of the
    new size reading as short.
    """
    source.rows = sales(1, 2)

    _, advanced = _next_chunk(source, state(), 2)

    assert advanced.dump_done is False


def test_reading_a_chunk_leaves_the_state_it_was_given_alone(source):
    source.rows = sales(1, 2, 3)
    carried = state(chunk_key=7)

    _next_chunk(source, carried, 3)

    assert carried.chunk_key == 7
    assert carried.dump_done is False


def test_refuses_a_leading_key_that_repeats_inside_a_chunk(source):
    """Rows sharing a key would be split across chunks and the ones past the boundary
    dropped."""
    source.rows = DataFrame({"sale_id": [1, 2, 2], "amount": [1, 1, 1]})

    with pytest.raises(ValueError, match="not unique"):
        _next_chunk(source, state(), 3)


def test_refuses_a_leading_key_that_is_not_an_integer(source):
    source.rows = DataFrame({"sale_id": ["a", "b"], "amount": [1, 1]})

    with pytest.raises(TypeError, match="integer"):
        _next_chunk(source, state(), 2)


def test_refuses_a_boolean_leading_key(source):
    """A bool is an int as far as isinstance is concerned, and would produce a nonsense
    key."""
    source.rows = DataFrame({"sale_id": [True, False], "amount": [1, 1]})

    with pytest.raises(TypeError, match="integer"):
        _next_chunk(source, state(), 2)


# ---------------------------------------------------------------------------
# _supersede — the log wins, because its image is the newer one
# ---------------------------------------------------------------------------


def test_a_chunk_survives_a_window_that_held_no_events():
    merged = _supersede(sales(1, 2, 3), None, SPEC)

    assert merged.to_dicts() == sales(1, 2, 3).to_dicts()


def test_a_chunk_survives_an_empty_window_frame():
    merged = _supersede(sales(1, 2, 3), events(), SPEC)

    assert merged.to_dicts() == sales(1, 2, 3).to_dicts()


def test_drops_the_chunk_rows_the_window_already_carries():
    """The event is the newer image of that row; the chunk's copy is stale."""
    merged = _supersede(sales(1, 2, 3), events(2), SPEC)

    assert merged["sale_id"].to_list() == [1, 3]


def test_keeps_the_chunk_in_key_order():
    """Chunks arrive ordered by primary key, and an unordered join would lose that."""
    merged = _supersede(sales(1, 2, 3, 4, 5, 6), events(3), SPEC)

    assert merged["sale_id"].to_list() == [1, 2, 4, 5, 6]


def test_a_window_that_covers_the_whole_chunk_leaves_nothing():
    assert _supersede(sales(1, 2), events(1, 2), SPEC).is_empty()


def test_a_row_touched_twice_in_the_window_is_dropped_once():
    """An insert then an update inside one window is two events for one key."""
    merged = _supersede(sales(1, 2, 3), events(2, 2), SPEC)

    assert merged["sale_id"].to_list() == [1, 3]


def test_events_for_rows_outside_the_chunk_change_nothing():
    merged = _supersede(sales(1, 2), events(90, 91), SPEC)

    assert merged["sale_id"].to_list() == [1, 2]


def test_a_window_that_supersedes_nothing_hands_back_the_same_frame():
    """
    Not merely an equal frame — the same one. The anti-join would return these rows
    unchanged while materialising every column to do it, which on a wide chunk costs as
    much memory again as the chunk itself. Identity is the assertion because it is the
    only one that fails if that copy comes back.
    """
    chunk = sales(1, 2, 3)

    assert _supersede(chunk, events(90, 91), SPEC) is chunk


def test_a_window_that_supersedes_something_still_rebuilds():
    """The short circuit must not swallow a real overlap."""
    chunk = sales(1, 2, 3)

    merged = _supersede(chunk, events(2), SPEC)

    assert merged is not chunk
    assert merged["sale_id"].to_list() == [1, 3]


def test_the_merged_chunk_keeps_the_table_columns():
    """The window's metadata columns must not leak into the chunk's schema."""
    merged = _supersede(sales(1, 2), events(1), SPEC)

    assert merged.columns == ["sale_id", "amount"]


def test_matches_on_every_primary_key_column():
    """A composite key must match on the whole key, not just its leading column."""
    spec = SPEC.model_copy(update={"pk_columns": ["tenant_id", "sale_id"]})
    chunk = DataFrame({"tenant_id": [1, 1, 2], "sale_id": [7, 8, 7]})
    window = DataFrame(
        {"operation": [4], "tenant_id": [1], "sale_id": [7], "amount": [99]}
    )

    merged = _supersede(chunk, window, spec)

    assert merged.to_dicts() == [
        {"tenant_id": 1, "sale_id": 8},
        {"tenant_id": 2, "sale_id": 7},
    ]


# ---------------------------------------------------------------------------
# The dump interleaved with the log
# ---------------------------------------------------------------------------


def test_closes_the_window_after_the_chunk_it_brackets(source):
    """
    The window has to cover the chunk scan. Reading it first would leave every write
    made during the scan unaccounted for by either side.
    """
    source.row_script = [sales(1, 2), sales(3)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    full_run(source, state(), chunk_size=2)

    reads = [
        call for call in source.calls if call in ("read_table", "read_event_log")
    ]

    assert reads[:4] == [
        "read_table",
        "read_event_log",
        "read_table",
        "read_event_log",
    ]


def test_emits_the_window_before_the_chunk_it_brackets(source):
    source.row_script = [sales(1, 2)]
    source.event_script = [events(50)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert len(frames) == 1
    assert frames[0]["sale_id"].to_list() == [50, 1, 2]


def test_the_window_supersedes_the_chunk_rows_it_covers(source):
    source.row_script = [sales(1, 2, 3), sales(4)]
    source.event_script = [events(2), None]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    frames, _ = full_run(source, state(), chunk_size=3)

    assert [frame["sale_id"].to_list() for frame in frames] == [[2, 1, 3], [4]]


def test_chunk_frames_are_stamped_into_the_event_shape(source):
    """A run yields one schema, so a consumer can stack every frame it gets."""
    source.row_script = [sales(1, 2)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert frames[0].columns[:3] == ["start_lsn", "operation", "commit_timestamp"]


def test_chunk_frames_are_stamped_even_when_the_window_was_empty(source):
    """The early return for an empty window used to skip straight past the stamping."""
    source.row_script = [sales(1, 2)]
    source.event_script = [None]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert frames[0]["operation"].to_list() == [0, 0]


def test_a_chunk_is_bracketed_by_two_watermarks(source):
    """
    The algorithm, in the order it has to happen: a watermark, the chunk scan, a second
    watermark, then the wait that makes the second one mean something. Only then may
    the window close.
    """
    source.row_script = [sales(1, 2)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    full_run(source, state(), chunk_size=2)

    assert source.calls[:5] == [
        "watermark",
        "read_table",
        "watermark",
        "await_watermark",
        "read_event_log",
    ]


def test_the_chunk_is_awaited_on_the_watermark_taken_after_it(source):
    """The one before the scan cannot prove the scan's own changes were captured."""
    source.row_script = [sales(1)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    full_run(source, state(), chunk_size=2)

    assert source.awaited == [source.watermarks[1]]


def test_chunk_frames_are_dated_from_the_watermark_before_the_chunk(source):
    """Dated from the low watermark, not the high — that one is taken after the read."""
    source.row_script = [sales(1, 2)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert frames[0]["commit_timestamp"].to_list() == [source.watermarks[0]] * 2


def test_a_chunk_wholly_superseded_is_not_emitted(source):
    """Yielding an empty frame would make a consumer handle a case that means nothing."""
    source.row_script = [sales(1, 2)]
    source.event_script = [events(1, 2)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert [frame["sale_id"].to_list() for frame in frames] == [[1, 2]]


def test_walks_the_whole_table_and_then_leaves(source):
    """
    A caller after the table and no more stops on ``dump_done``: events that landed
    after the last chunk are the log's business, and wait for whatever tails it.
    """
    source.row_script = [sales(1, 2), sales(3)]
    source.event_script = [None, None, events(77)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400), lsn(500)]

    frames, carried = dump_only(source, state(), chunk_size=2)

    assert [frame["sale_id"].to_list() for frame in frames] == [[1, 2], [3]]
    assert carried.dump_done is True


def test_a_dump_of_an_empty_table_still_drains_the_log(source):
    source.row_script = [sales()]
    source.event_script = [events(5)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    frames, _ = full_run(source, state(), chunk_size=2)

    assert [frame["sale_id"].to_list() for frame in frames] == [[5]]


def test_the_dump_reads_one_window_per_chunk_and_no_more(source):
    """Each chunk gets the window bracketing it; none of them tails on its own."""
    source.row_script = [sales(1, 2), sales(3)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    dump_only(source, state(), chunk_size=2)

    assert source.calls.count("read_event_log") == 2


def test_walking_the_table_takes_as_many_calls_as_it_takes(source):
    source.row_script = [sales(1, 2), sales(3, 4), sales(5)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    frames, _ = dump_only(source, state(), chunk_size=2)

    assert [frame["sale_id"].to_list() for frame in frames] == [[1, 2], [3, 4], [5]]


# ---------------------------------------------------------------------------
# Carrying on — the state is the handoff, and the only one there is
# ---------------------------------------------------------------------------


def test_a_batch_hands_back_where_the_next_one_carries_on_from(source):
    source.rows = sales(1, 2, 3)
    source.max_lsn = lsn(200)

    result = dblog(source, **TARGET, state=state(last_lsn=lsn(10)), chunk_size=3)

    assert result.state.chunk_key == 4
    assert result.state.last_lsn == lsn(201)


def test_carrying_the_state_resumes_the_table_walk_where_it_stopped(source):
    source.rows = sales(7, 8, 9)

    dblog(source, **TARGET, state=state(chunk_key=7), chunk_size=3)

    assert source.read_table_calls[0][1] == 7


def test_carrying_the_state_resumes_the_log_where_it_stopped(source):
    source.max_lsn = lsn(900)

    dblog(source, **TARGET, state=state(last_lsn=lsn(500)), chunk_size=3)

    assert source.event_log_calls[0][1] == lsn(500)


def test_carrying_the_older_state_reads_the_same_batch_again(source):
    """
    A frame received but never saved is read again by the next call given the older
    state. That is the at-least-once guarantee, and it is the whole reason the position
    comes back rather than being written from in here.
    """
    source.rows = sales(1, 2, 3)
    source.max_lsn = lsn(200)
    carried = state(last_lsn=lsn(10))

    first = dblog(source, **TARGET, state=carried, chunk_size=3)
    again = dblog(source, **TARGET, state=carried, chunk_size=3)

    assert first.frame is not None and again.frame is not None
    assert first.frame["sale_id"].to_list() == again.frame["sale_id"].to_list()
    assert source.read_table_calls[0][1] == source.read_table_calls[1][1]


def test_a_run_writes_nothing_of_its_own(source, tmp_path, monkeypatch):
    """
    There is nowhere for it to write to. The old default was a JSON file in the working
    directory, so this pins that no such thing reappears.
    """
    monkeypatch.chdir(tmp_path)
    source.row_script = [sales(1, 2, 3), sales(4)]
    source.max_lsn_script = [lsn(200), lsn(300)]

    full_run(source, state(), chunk_size=3)

    assert list(tmp_path.iterdir()) == []


def test_the_state_a_call_was_given_is_never_mutated(source):
    source.rows = sales(1, 2, 3)
    source.max_lsn = lsn(200)
    carried = state(last_lsn=lsn(10))

    dblog(source, **TARGET, state=carried, chunk_size=3)

    assert carried.chunk_key is None
    assert carried.last_lsn == lsn(10)
    assert carried.dump_done is False


# ---------------------------------------------------------------------------
# Tailing — a finished dump reads the log alone
# ---------------------------------------------------------------------------


def test_a_finished_dump_never_reads_the_table(source):
    source.max_lsn = lsn(200)

    dblog(source, **TARGET, state=state(dump_done=True))

    assert source.read_table_calls == []


def test_a_finished_dump_reads_the_window_from_where_it_stopped(source):
    """The handoff: the position a walked table left behind opens the tail."""
    source.max_lsn = lsn(900)
    source.event_script = [events(77)]

    result = dblog(source, **TARGET, state=state(last_lsn=lsn(500), dump_done=True))

    assert source.event_log_calls == [(SPEC, lsn(500), lsn(900))]
    assert result.frame is not None
    assert result.frame["sale_id"].to_list() == [77]


def test_a_walked_table_hands_its_position_to_the_tail(source):
    """
    End to end, in the shape a caller uses: walk the table, keep the state, then read
    the log alone with it and nothing else.
    """
    source.row_script = [sales(1, 2), sales(3)]
    source.event_script = [None, None, events(77)]
    source.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    _, walked = dump_only(source, state(), chunk_size=2)
    reads = len(source.read_table_calls)

    result = dblog(source, **TARGET, state=walked)

    assert result.frame is not None
    assert result.frame["sale_id"].to_list() == [77]
    assert len(source.read_table_calls) == reads  # no further table read


def test_a_finished_dump_takes_no_watermarks(source):
    """There is no chunk scan to bracket, so the barrier has nothing to prove."""
    source.max_lsn = lsn(200)

    dblog(source, **TARGET, state=state(dump_done=True))

    assert source.awaited == []


def test_a_tail_that_found_nothing_still_advances(source):
    """Which is why the caller has to keep the state off a result whose frame is None."""
    source.max_lsn = lsn(200)
    source.events = None

    result = dblog(source, **TARGET, state=state(last_lsn=lsn(10), dump_done=True))

    assert result.frame is None
    assert result.state.last_lsn == lsn(201)


def test_a_tail_reads_the_source_there_and_then(source):
    """Polling belongs to the caller: one call, one window, as of now."""
    source.max_lsn_script = [lsn(200), lsn(300)]
    carried = state(last_lsn=lsn(10), dump_done=True)

    first = dblog(source, **TARGET, state=carried)
    second = dblog(source, **TARGET, state=first.state)

    assert source.event_log_calls == [
        (SPEC, lsn(10), lsn(200)),
        (SPEC, lsn(201), lsn(300)),
    ]
    assert second.state.last_lsn == lsn(301)


def test_a_tail_does_not_chase_a_log_end_that_keeps_moving(source):
    """The window closes at the max LSN read once, not at wherever it has got to since."""
    source.max_lsn_script = [lsn(200)]
    source.events = None

    result = dblog(source, **TARGET, state=state(last_lsn=lsn(10), dump_done=True))

    assert source.event_log_calls == [(SPEC, lsn(10), lsn(200))]
    assert result.state.last_lsn == lsn(201)


# ---------------------------------------------------------------------------
# Retention — checked every call, because a saved state is the ordinary input
# ---------------------------------------------------------------------------


def test_refuses_a_position_that_aged_out_of_the_log(source):
    source.min_lsn = lsn(500)

    with pytest.raises(CdcRetentionExpiredError, match="dbo.sales"):
        dblog(source, **TARGET, state=state(last_lsn=lsn(10)))


def test_a_position_exactly_on_the_floor_is_readable(source):
    source.min_lsn = lsn(500)
    source.max_lsn = lsn(900)

    dblog(source, **TARGET, state=state(last_lsn=lsn(500), dump_done=True))

    assert source.event_log_calls == [(SPEC, lsn(500), lsn(900))]


def test_the_expired_position_is_caught_before_anything_is_read(source):
    """The gap cannot be read, so reading part of it would only muddle the recovery."""
    source.min_lsn = lsn(500)

    with pytest.raises(CdcRetentionExpiredError):
        dblog(source, **TARGET, state=state(last_lsn=lsn(10)))

    assert source.event_log_calls == []
    assert source.read_table_calls == []


def test_a_state_that_aged_out_is_checked_on_every_call_not_only_the_first(source):
    """
    A state persisted days ago is the ordinary input now, so a position falling below
    the floor is something that happens while nobody is looking.
    """
    source.max_lsn = lsn(900)
    carried = state(last_lsn=lsn(500), dump_done=True)
    first = dblog(source, **TARGET, state=carried)

    source.min_lsn = lsn(5000)

    with pytest.raises(CdcRetentionExpiredError):
        dblog(source, **TARGET, state=first.state)


def test_a_fresh_run_after_an_expired_one_opens_above_the_floor(source):
    """The recovery the error names: no state, and the run opens at the present."""
    source.min_lsn = lsn(5000)
    source.max_lsn = lsn(50)

    result = dblog(source, **TARGET, chunk_size=2)

    assert result.state.last_lsn >= lsn(5000)


# ---------------------------------------------------------------------------
# Re-inspecting — the spec in a long-lived state goes stale
# ---------------------------------------------------------------------------


def stale(**overrides) -> RunState:
    """A state whose spec was read two days ago."""
    return state(last_inspect=datetime.now(UTC) - timedelta(days=2), **overrides)


def test_a_fresh_spec_is_not_re_read(source):
    dblog(source, **TARGET, state=state(dump_done=True))

    assert source.inspect_calls == []


def test_a_stale_spec_is_re_read(source):
    dblog(source, **TARGET, state=stale(dump_done=True))

    assert source.inspect_calls == [("dbo", "sales")]


def test_re_inspecting_restamps_the_state(source):
    """Otherwise every call from here on re-inspects."""
    before = datetime.now(UTC)

    result = dblog(source, **TARGET, state=stale(dump_done=True))

    assert result.state.last_inspect >= before


def test_a_state_carried_on_after_a_re_inspect_does_not_inspect_again(source):
    first = dblog(source, **TARGET, state=stale(dump_done=True))

    dblog(source, **TARGET, state=first.state)

    assert source.inspect_calls == [("dbo", "sales")]


def test_never_re_inspects_when_told_not_to(source):
    """Which pins the run to the table as it was when the run opened."""
    dblog(source, **TARGET, state=stale(dump_done=True), inspect_every=None)

    assert source.inspect_calls == []


def test_a_zero_interval_re_inspects_every_call(source):
    carried = state(dump_done=True)

    first = dblog(source, **TARGET, state=carried, inspect_every=timedelta(0))
    dblog(source, **TARGET, state=first.state, inspect_every=timedelta(0))

    assert len(source.inspect_calls) == 2


def test_a_column_added_to_the_source_alone_is_only_a_warning(source, caplog):
    """
    CDC keeps the set its capture instance was created with, so neither read projects
    the new column and no frame changes shape.
    """
    source.spec_script = [SPEC_SOURCE_WIDENED]

    with caplog.at_level("WARNING"):
        result = dblog(source, **TARGET, state=stale(dump_done=True))

    assert "note" in caplog.text
    assert result.state.spec == SPEC_SOURCE_WIDENED


def test_a_column_the_capture_instance_now_carries_stops_the_run(source):
    """
    Frames after this point would not stack with the ones already emitted, and a
    consumer finds that out at its own concat long after the batch that caused it was
    written.
    """
    source.spec_script = [SPEC_CAPTURE_WIDENED]

    with pytest.raises(SchemaChangedError, match="note"):
        dblog(source, **TARGET, state=stale(dump_done=True))


def test_a_re_keyed_table_stops_the_run(source):
    """The chunk key is a position in the old key space, and events can no longer be
    matched against chunk rows on the same columns."""
    source.spec_script = [SPEC_REKEYED]

    with pytest.raises(SchemaChangedError, match="amount"):
        dblog(source, **TARGET, state=stale(chunk_key=7))


def test_a_schema_change_is_caught_before_anything_is_read(source):
    source.spec_script = [SPEC_CAPTURE_WIDENED]

    with pytest.raises(SchemaChangedError):
        dblog(source, **TARGET, state=stale())

    assert source.event_log_calls == []
    assert source.read_table_calls == []


def test_a_table_that_lost_its_capture_instance_stops_the_run(source):
    source.spec_script = [SPEC.model_copy(update={"capture_instance": None})]

    with pytest.raises(ValueError, match="capture instance"):
        dblog(source, **TARGET, state=stale(dump_done=True))


def test_the_re_read_spec_is_what_the_reads_use(source):
    """A spec adopted but not passed to the reads would be a re-inspect that did
    nothing."""
    source.spec_script = [SPEC_SOURCE_WIDENED]
    source.max_lsn = lsn(200)

    dblog(source, **TARGET, state=stale(dump_done=True))

    assert source.event_log_calls == [(SPEC_SOURCE_WIDENED, lsn(10), lsn(200))]


# ---------------------------------------------------------------------------
# The state as a value — what a run hands back has to survive being written down
#
# That it round-trips at all is test_state.py's job; these are about a state that
# went through a file still driving the algorithm.
# ---------------------------------------------------------------------------


def test_what_a_run_hands_back_round_trips_through_json(source):
    source.rows = sales(1, 2, 3)
    source.max_lsn = lsn(200)
    result = dblog(source, **TARGET, state=state(), chunk_size=3)

    restored = RunState.model_validate_json(result.state.model_dump_json())

    assert restored == result.state


def test_a_restored_state_carries_on_where_the_run_left_off(source):
    source.rows = sales(7, 8, 9)
    source.max_lsn = lsn(900)
    carried = RunState.model_validate_json(
        state(chunk_key=7, last_lsn=lsn(500)).model_dump_json()
    )

    dblog(source, **TARGET, state=carried, chunk_size=3)

    assert source.read_table_calls[0][1] == 7
    assert source.event_log_calls[0][1] == lsn(500)
