"""
DBLog orchestrator tests.

None of this needs a database. ``DBLog`` only ever talks to the ten primitives of
the ``SourceConnector`` Protocol, so a stub implementing that Protocol is enough to
assert the orchestration — which is the point of typing the attribute as the
Protocol rather than as ``MSSQLConnector``.

Whether those primitives actually work against SQL Server is ``test_mssql.py``'s
job, and it is already answered there.
"""

import logging
from datetime import datetime

import pytest
from polars import DataFrame

import pydblog.dblog
from pydblog.connectors.types import LSN, TableSpec
from pydblog.dblog import DEFAULT_CHUNK_SIZE, CdcRetentionExpiredError, DBLog
from pydblog.state import DumpState

CONNECTION = {
    "source_type": "mssql",
    "host": "localhost",
    "port": "1433",
    "user": "sa",
    "password": "p",
    "database": "dblog_lab",
}


SPEC = TableSpec(
    source_schema="dbo",
    source_table="sales",
    pk_columns=["sale_id"],
    business_columns=["sale_id", "amount"],
    capture_instance="dbo_sales",
)


def lsn(value: int) -> LSN:
    """An LSN is ten bytes, big-endian, so ordering by value is ordering by bytes."""
    return value.to_bytes(10, "big")


class StubConnector:
    """
    A scriptable ``SourceConnector`` that records what the algorithm asked it.

    ``max_lsn`` and ``events`` are what the next call returns; tests set them to
    stage a scenario. The ``*_calls`` lists are what the algorithm actually asked
    for, which is what the assertions are about.
    """

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.connects = 0
        self.closes = 0
        self.max_lsn: LSN = lsn(100)
        self.min_lsn: LSN = lsn(1)
        self.spec: TableSpec | None = None
        self.events: DataFrame | None = None
        # Scripts, for when one call is not enough: each drains one entry per call
        # and the source stops moving once it runs dry.
        self.max_lsn_script: list[LSN] = []
        self.event_script: list[DataFrame | None] | None = None
        self.rows: DataFrame = DataFrame()
        self.row_script: list[DataFrame] | None = None
        self.event_log_calls: list[tuple[TableSpec, LSN, LSN]] = []
        self.read_table_calls: list[tuple[TableSpec, int | None, int | None, int]] = []
        self.inspect_calls: list[tuple[str, str]] = []
        # Every read in the order it happened. The interleave is as much about
        # sequence as about results: the window has to close after the chunk scan.
        self.calls: list[str] = []

    def connect(self) -> None:
        self.connects += 1

    def close(self) -> None:
        self.closes += 1

    def get_max_lsn(self) -> LSN:
        if self.max_lsn_script:
            self.max_lsn = self.max_lsn_script.pop(0)
        return self.max_lsn

    def get_min_lsn(self, capture_instance: str) -> LSN:
        return self.min_lsn

    def inspect(self, schema: str, table: str) -> TableSpec:
        self.inspect_calls.append((schema, table))
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

    def map_lsn_to_timestamp(self, value: LSN) -> datetime:
        raise NotImplementedError

    def increment_lsn(self, value: LSN) -> LSN:
        return lsn(int.from_bytes(value, "big") + 1)


class StubStore:
    """An in-memory StateStore, so tests never touch the filesystem."""

    def __init__(self) -> None:
        self.states: dict[str, DumpState] = {}
        self.saves: list[DumpState] = []

    def load(self, dump: str) -> DumpState | None:
        return self.states.get(dump)

    def save(self, state: DumpState) -> None:
        self.states[state.dump] = state
        self.saves.append(state)

    def clear(self, dump: str) -> None:
        self.states.pop(dump, None)


@pytest.fixture
def factory_calls(monkeypatch) -> list[dict]:
    """
    Swap ``build_connector`` for one that yields stubs and records its arguments.

    Returns the list the calls land in; the stub itself is reachable through
    ``DBLog._connector``.
    """
    calls: list[dict] = []

    def fake_build_connector(**kwargs) -> StubConnector:
        calls.append(kwargs)
        return StubConnector(**kwargs)

    monkeypatch.setattr(pydblog.dblog, "build_connector", fake_build_connector)
    return calls


# ---------------------------------------------------------------------------
# Construction — the connector comes from the factory, never from an import
# ---------------------------------------------------------------------------


def test_builds_its_connector_through_the_factory(factory_calls):
    dblog = DBLog(**CONNECTION)

    assert factory_calls == [CONNECTION]
    assert isinstance(dblog._connector, StubConnector)


def test_passes_extra_arguments_through_to_the_factory(factory_calls):
    DBLog(**CONNECTION, application_name="pytest")

    assert factory_calls[0]["application_name"] == "pytest"


def test_chunk_size_does_not_reach_the_factory(factory_calls):
    """It is the algorithm's knob, not the connection's."""
    DBLog(**CONNECTION, chunk_size=500)

    assert "chunk_size" not in factory_calls[0]


def test_chunk_size_defaults(factory_calls):
    assert DBLog(**CONNECTION)._chunk_size == DEFAULT_CHUNK_SIZE


@pytest.mark.parametrize("chunk_size", [0, -1, -1000])
def test_rejects_a_non_positive_chunk_size(factory_calls, chunk_size):
    with pytest.raises(ValueError, match="chunk_size"):
        DBLog(**CONNECTION, chunk_size=chunk_size)


# ---------------------------------------------------------------------------
# State — nothing is known until a run starts
# ---------------------------------------------------------------------------


def test_verbose_turns_the_detail_on(factory_calls):
    DBLog(**CONNECTION, verbose=True)

    assert logging.getLogger("pydblog").level == logging.DEBUG


def test_staying_quiet_is_the_default(factory_calls):
    """A library that configures logging unasked overrides its host's choices."""
    package = logging.getLogger("pydblog")
    package.setLevel(logging.NOTSET)

    DBLog(**CONNECTION)

    assert package.level == logging.NOTSET


def test_verbose_does_not_reach_the_connector(factory_calls):
    DBLog(**CONNECTION, verbose=True)

    assert "verbose" not in factory_calls[0]


def test_starts_with_empty_state(factory_calls):
    dblog = DBLog(**CONNECTION)

    assert dblog._spec is None
    assert dblog._last_lsn is None
    assert dblog._chunk_key is None
    assert dblog._dump_done is False


def test_constructing_does_not_connect(factory_calls):
    dblog = DBLog(**CONNECTION)

    assert dblog._connector.connects == 0


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_connect_delegates_to_the_connector(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog.connect()

    assert dblog._connector.connects == 1
    assert dblog._connector.closes == 0


def test_close_delegates_to_the_connector(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog.close()

    assert dblog._connector.closes == 1


def test_context_manager_connects_and_closes(factory_calls):
    with DBLog(**CONNECTION) as dblog:
        assert dblog._connector.connects == 1
        assert dblog._connector.closes == 0

    assert dblog._connector.closes == 1


def test_context_manager_yields_itself(factory_calls):
    dblog = DBLog(**CONNECTION)

    with dblog as entered:
        assert entered is dblog


def test_context_manager_closes_when_the_body_raises(factory_calls):
    dblog = DBLog(**CONNECTION)

    with pytest.raises(RuntimeError):
        with dblog:
            raise RuntimeError("boom")

    assert dblog._connector.closes == 1


def test_context_manager_does_not_swallow_the_exception(factory_calls):
    """``__exit__`` must not return a truthy value."""
    dblog = DBLog(**CONNECTION)

    with pytest.raises(ValueError):
        with dblog:
            raise ValueError("propagate me")


# ---------------------------------------------------------------------------
# _read_window — one pass over the log, from the last LSN to the current max LSN
# ---------------------------------------------------------------------------

def seeded(factory_calls, last_lsn: int = 10, max_lsn: int = 100) -> DBLog:
    """A DBLog with the run state _read_window expects, staged by hand."""
    dblog = DBLog(**CONNECTION)
    dblog._spec = SPEC
    dblog._last_lsn = lsn(last_lsn)
    dblog._connector.max_lsn = lsn(max_lsn)
    return dblog


def test_reads_from_the_last_lsn_up_to_the_current_max_lsn(factory_calls):
    dblog = seeded(factory_calls, last_lsn=10, max_lsn=100)

    dblog._read_window()

    assert dblog._connector.event_log_calls == [(SPEC, lsn(10), lsn(100))]


def test_returns_the_events_the_connector_read(factory_calls):
    dblog = seeded(factory_calls)
    dblog._connector.events = DataFrame({"sale_id": [1, 2]})

    window = dblog._read_window()

    assert window is not None
    assert window.to_dicts() == [{"sale_id": 1}, {"sale_id": 2}]


def test_advances_the_last_lsn_one_past_the_window(factory_calls):
    """The CDC read is inclusive on both bounds, so reopening at the high LSN
    would deliver every event sitting on the boundary a second time."""
    dblog = seeded(factory_calls, last_lsn=10, max_lsn=100)

    dblog._read_window()

    assert dblog._last_lsn == lsn(101)


def test_reads_the_window_whose_bounds_meet(factory_calls):
    """A last LSN exactly at the max LSN is a one-LSN window, not an empty one."""
    dblog = seeded(factory_calls, last_lsn=100, max_lsn=100)

    dblog._read_window()

    assert dblog._connector.event_log_calls == [(SPEC, lsn(100), lsn(100))]
    assert dblog._last_lsn == lsn(101)


def test_returns_none_when_the_window_holds_no_events(factory_calls):
    dblog = seeded(factory_calls)
    dblog._connector.events = None

    assert dblog._read_window() is None


def test_advances_the_last_lsn_even_when_the_window_was_empty(factory_calls):
    """Otherwise a quiet table re-scans the same widening range forever."""
    dblog = seeded(factory_calls, last_lsn=10, max_lsn=100)
    dblog._connector.events = None

    dblog._read_window()

    assert dblog._last_lsn == lsn(101)


def test_does_not_query_when_the_last_lsn_is_past_the_max_lsn(factory_calls):
    """The steady state of a database with no new commits: it must cost nothing."""
    dblog = seeded(factory_calls, last_lsn=101, max_lsn=100)

    assert dblog._read_window() is None
    assert dblog._connector.event_log_calls == []


def test_leaves_the_last_lsn_alone_when_there_is_nothing_new(factory_calls):
    dblog = seeded(factory_calls, last_lsn=101, max_lsn=100)

    dblog._read_window()

    assert dblog._last_lsn == lsn(101)


@pytest.mark.parametrize("missing", ["_spec", "_last_lsn"])
def test_refuses_to_read_a_window_before_the_run_state_is_seeded(
    factory_calls, missing
):
    dblog = seeded(factory_calls)
    setattr(dblog, missing, None)

    with pytest.raises(RuntimeError, match="not started"):
        dblog._read_window()


# ---------------------------------------------------------------------------
# Seeding — where a run starts reading from
# ---------------------------------------------------------------------------


def drain(dblog: DBLog, **kwargs) -> list[DataFrame]:
    """Run events-only to exhaustion. run() is lazy, so nothing happens until this."""
    return list(dblog.run("dbo", "sales", **kwargs))


def test_inspects_the_table_it_was_asked_for(factory_calls):
    dblog = DBLog(**CONNECTION)

    drain(dblog)

    assert dblog._connector.inspect_calls == [("dbo", "sales")]
    assert dblog._spec == SPEC


def test_reads_everything_the_log_still_holds_when_no_lsn_is_given(factory_calls):
    """No starting point asked for means no events skipped: begin at the floor."""
    dblog = DBLog(**CONNECTION)
    dblog._connector.max_lsn = lsn(100)
    dblog._connector.min_lsn = lsn(1)

    drain(dblog)

    assert dblog._connector.event_log_calls[0][1] == lsn(1)


def test_the_floor_itself_is_read(factory_calls):
    """
    It is the first LSN retained, not the last one gone, and the CDC read is
    inclusive — so an event sitting exactly on it is one of the events to read.
    """
    dblog = DBLog(**CONNECTION)
    dblog._connector.min_lsn = lsn(40)
    dblog._connector.max_lsn = lsn(40)

    drain(dblog)

    assert dblog._connector.event_log_calls == [(SPEC, lsn(40), lsn(40))]


def test_reads_nothing_when_the_floor_is_above_the_max_lsn(factory_calls):
    """
    A capture instance enabled moments ago has a start_lsn the database-wide max
    has not reached yet. There is nothing to read, and asking would be asking CDC
    for events it never held.
    """
    dblog = DBLog(**CONNECTION)
    dblog._connector.max_lsn = lsn(50)
    dblog._connector.min_lsn = lsn(200)

    drain(dblog)

    assert dblog._last_lsn == lsn(200)
    assert dblog._connector.event_log_calls == []


def test_starts_where_the_caller_asked(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog._connector.min_lsn = lsn(1)
    dblog._connector.max_lsn = lsn(100)

    drain(dblog, from_lsn=lsn(42))

    assert dblog._connector.event_log_calls[0][1] == lsn(42)


def test_accepts_a_start_exactly_on_the_retention_floor(factory_calls):
    """The floor is still readable; it is the first LSN CDC retains, not the last."""
    dblog = DBLog(**CONNECTION)
    dblog._connector.min_lsn = lsn(40)

    drain(dblog, from_lsn=lsn(40))

    assert dblog._connector.event_log_calls[0][1] == lsn(40)


def test_refuses_a_start_that_aged_out_of_retention(factory_calls):
    """Reading from the floor instead would skip events with no signal at all."""
    dblog = DBLog(**CONNECTION)
    dblog._connector.min_lsn = lsn(40)

    with pytest.raises(CdcRetentionExpiredError, match="no longer retains"):
        drain(dblog, from_lsn=lsn(39))


def test_refuses_a_table_that_is_not_captured(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog._connector.spec = SPEC.model_copy(update={"capture_instance": None})

    with pytest.raises(ValueError, match="capture instance"):
        drain(dblog)


def test_starting_a_run_clears_the_dump_state(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog._chunk_key = 999
    dblog._dump_done = True

    drain(dblog)

    assert dblog._chunk_key is None
    assert dblog._dump_done is False


# ---------------------------------------------------------------------------
# The dump name — the identity progress is recorded under, not a label
# ---------------------------------------------------------------------------


def test_records_the_dump_it_was_asked_to_run(factory_calls):
    dblog = DBLog(**CONNECTION, state_store=StubStore())

    list(dblog.run("dbo", "sales", dump="sales-backfill"))

    assert dblog._dump == "sales-backfill"


def test_an_unnamed_run_has_no_dump(factory_calls):
    dblog = DBLog(**CONNECTION)

    drain(dblog)

    assert dblog._dump is None


@pytest.mark.parametrize("name", ["", "   ", "\t"])
def test_refuses_a_blank_dump_name(factory_calls, name):
    """A dump keyed on blank is indistinguishable from an unnamed run."""
    dblog = DBLog(**CONNECTION, state_store=StubStore())

    with pytest.raises(ValueError, match="blank"):
        list(dblog.run("dbo", "sales", dump=name))


def test_a_blank_dump_name_is_caught_before_the_source_is_touched(factory_calls):
    dblog = DBLog(**CONNECTION)

    with pytest.raises(ValueError):
        list(dblog.run("dbo", "sales", dump=""))

    assert dblog._connector.inspect_calls == []


# ---------------------------------------------------------------------------
# _next_chunk — one keyset page of the table, sized by row count
# ---------------------------------------------------------------------------


def sales(*sale_ids: int) -> DataFrame:
    """A chunk of table rows, keyed the way SPEC says."""
    return DataFrame({"sale_id": list(sale_ids), "amount": [1] * len(sale_ids)})


def dumping(factory_calls, chunk_size: int = 3, chunk_key: int | None = None) -> DBLog:
    """A DBLog mid-dump, staged by hand, with a store that stays in memory."""
    dblog = DBLog(**CONNECTION, chunk_size=chunk_size, state_store=StubStore())
    dblog._spec = SPEC
    dblog._chunk_key = chunk_key
    return dblog


def test_first_chunk_reads_from_the_start_of_the_table(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.rows = sales(1, 2, 3)

    dblog._next_chunk()

    assert dblog._connector.read_table_calls == [(SPEC, None, None, 3)]


def test_later_chunks_read_from_where_the_last_one_stopped(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3, chunk_key=7)
    dblog._connector.rows = sales(7, 8, 9)

    dblog._next_chunk()

    assert dblog._connector.read_table_calls == [(SPEC, 7, None, 3)]


def test_returns_the_rows_it_read(factory_calls):
    dblog = dumping(factory_calls)
    dblog._connector.rows = sales(1, 2, 3)

    chunk = dblog._next_chunk()

    assert chunk is not None
    assert chunk.to_dicts() == sales(1, 2, 3).to_dicts()


def test_advances_past_the_last_row_of_the_chunk(factory_calls):
    """start_pk is inclusive, so without the +1 the last row opens the next chunk."""
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.rows = sales(1, 5, 9)

    dblog._next_chunk()

    assert dblog._chunk_key == 10


def test_a_full_chunk_leaves_the_dump_running(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.rows = sales(1, 2, 3)

    dblog._next_chunk()

    assert dblog._dump_done is False


def test_a_short_chunk_ends_the_dump(factory_calls):
    """Fewer rows than asked for means the table had no more to give."""
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.rows = sales(1, 2)

    chunk = dblog._next_chunk()

    assert chunk is not None
    assert chunk.height == 2
    assert dblog._dump_done is True


def test_an_empty_chunk_ends_the_dump_and_yields_nothing(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3, chunk_key=99)
    dblog._connector.rows = sales()

    assert dblog._next_chunk() is None
    assert dblog._dump_done is True


def test_an_empty_chunk_does_not_move_the_chunk_key(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3, chunk_key=99)
    dblog._connector.rows = sales()

    dblog._next_chunk()

    assert dblog._chunk_key == 99


def test_reads_nothing_once_the_dump_is_done(factory_calls):
    dblog = dumping(factory_calls)
    dblog._dump_done = True

    assert dblog._next_chunk() is None
    assert dblog._connector.read_table_calls == []


def test_walks_the_table_one_chunk_at_a_time(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(4, 8), sales(9)]

    chunks = [dblog._next_chunk(), dblog._next_chunk(), dblog._next_chunk()]

    assert [chunk.height if chunk is not None else None for chunk in chunks] == [2, 2, 1]
    assert [call[1] for call in dblog._connector.read_table_calls] == [None, 3, 9]
    assert dblog._dump_done is True


def test_refuses_a_leading_key_that_repeats_inside_a_chunk(factory_calls):
    """
    read_table bounds on the leading key alone. If a chunk splits a key group,
    advancing past that key drops its remaining rows, and no log event exists to
    repair them — they were never modified.
    """
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.rows = sales(1, 2, 2)

    with pytest.raises(ValueError, match="not unique"):
        dblog._next_chunk()


def test_refuses_a_leading_key_that_is_not_an_integer(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.rows = DataFrame({"sale_id": ["a", "b"], "amount": [1, 1]})

    with pytest.raises(TypeError, match="integer"):
        dblog._next_chunk()


def test_refuses_a_boolean_leading_key(factory_calls):
    """isinstance(True, int) is True, so a bit column would slip through unguarded."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.rows = DataFrame({"sale_id": [False, True], "amount": [1, 1]})

    with pytest.raises(TypeError, match="integer"):
        dblog._next_chunk()


def test_refuses_to_read_a_chunk_before_the_run_state_is_seeded(factory_calls):
    dblog = DBLog(**CONNECTION)

    with pytest.raises(RuntimeError, match="not started"):
        dblog._next_chunk()


# ---------------------------------------------------------------------------
# _merge_chunk — the log wins, because its image is the newer one
# ---------------------------------------------------------------------------


def events(*sale_ids: int) -> DataFrame:
    """A window of change events, shaped the way read_event_log returns them."""
    return DataFrame(
        {
            "start_lsn": [lsn(n) for n in sale_ids],
            "operation": [4] * len(sale_ids),
            "sale_id": list(sale_ids),
            "amount": [99] * len(sale_ids),
        }
    )


def test_a_chunk_survives_a_window_that_held_no_events(factory_calls):
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2, 3), None)

    assert merged.to_dicts() == sales(1, 2, 3).to_dicts()


def test_a_chunk_survives_an_empty_window_frame(factory_calls):
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2, 3), events())

    assert merged.to_dicts() == sales(1, 2, 3).to_dicts()


def test_drops_the_chunk_rows_the_window_already_carries(factory_calls):
    """The event is the newer image of that row; the chunk's copy is stale."""
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2, 3), events(2))

    assert merged["sale_id"].to_list() == [1, 3]


def test_keeps_the_chunk_in_key_order(factory_calls):
    """Chunks arrive ordered by primary key, and an unordered join would lose that."""
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2, 3, 4, 5, 6), events(3))

    assert merged["sale_id"].to_list() == [1, 2, 4, 5, 6]


def test_a_window_that_covers_the_whole_chunk_leaves_nothing(factory_calls):
    dblog = dumping(factory_calls)

    assert dblog._merge_chunk(sales(1, 2), events(1, 2)).is_empty()


def test_a_row_touched_twice_in_the_window_is_dropped_once(factory_calls):
    """An insert then an update inside one window is two events for one key."""
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2, 3), events(2, 2))

    assert merged["sale_id"].to_list() == [1, 3]


def test_events_for_rows_outside_the_chunk_change_nothing(factory_calls):
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2), events(90, 91))

    assert merged["sale_id"].to_list() == [1, 2]


def test_the_merged_chunk_keeps_the_table_columns(factory_calls):
    """The window's metadata columns must not leak into the chunk's schema."""
    dblog = dumping(factory_calls)

    merged = dblog._merge_chunk(sales(1, 2), events(1))

    assert merged.columns == ["sale_id", "amount"]


def test_matches_on_every_primary_key_column(factory_calls):
    """A composite key must match on the whole key, not just its leading column."""
    dblog = dumping(factory_calls)
    dblog._spec = SPEC.model_copy(update={"pk_columns": ["tenant_id", "sale_id"]})
    chunk = DataFrame({"tenant_id": [1, 1, 2], "sale_id": [7, 8, 7]})
    window = DataFrame(
        {"operation": [4], "tenant_id": [1], "sale_id": [7], "amount": [99]}
    )

    merged = dblog._merge_chunk(chunk, window)

    assert merged.to_dicts() == [
        {"tenant_id": 1, "sale_id": 8},
        {"tenant_id": 2, "sale_id": 7},
    ]


# ---------------------------------------------------------------------------
# run(dump=...) — the dump interleaved with the log
# ---------------------------------------------------------------------------


def full_run(dblog: DBLog, name: str = "sales-backfill", **kwargs) -> list[DataFrame]:
    return list(dblog.run("dbo", "sales", dump=name, **kwargs))


def test_closes_the_window_after_the_chunk_it_brackets(factory_calls):
    """
    The window has to cover the chunk scan. Reading it first would leave every
    write made during the scan unaccounted for by either side.
    """
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(3)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    full_run(dblog, from_lsn=lsn(10))

    assert dblog._connector.calls[:4] == [
        "read_table",
        "read_event_log",
        "read_table",
        "read_event_log",
    ]


def test_emits_the_window_before_the_chunk_it_brackets(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2)]
    dblog._connector.event_script = [events(50)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300)]

    frames = full_run(dblog, from_lsn=lsn(10))

    assert frames[0]["sale_id"].to_list() == [50]
    assert frames[1]["sale_id"].to_list() == [1, 2]


def test_the_window_supersedes_the_chunk_rows_it_covers(factory_calls):
    dblog = dumping(factory_calls, chunk_size=3)
    dblog._connector.row_script = [sales(1, 2, 3), sales(4)]
    dblog._connector.event_script = [events(2), None]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    frames = full_run(dblog, from_lsn=lsn(10))

    assert [frame["sale_id"].to_list() for frame in frames] == [[2], [1, 3], [4]]


def test_a_chunk_wholly_superseded_is_not_emitted(factory_calls):
    """Yielding an empty frame would make a consumer handle a case that means nothing."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2)]
    dblog._connector.event_script = [events(1, 2)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300)]

    frames = full_run(dblog, from_lsn=lsn(10))

    assert [frame["sale_id"].to_list() for frame in frames] == [[1, 2]]


def test_walks_the_whole_table_and_then_leaves(factory_calls):
    """
    The run is over when the table is: events after the last chunk belong to the
    next run, which will pick them up from the recorded position.
    """
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(3)]
    dblog._connector.event_script = [None, None, events(77)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300), lsn(400), lsn(500)]

    frames = full_run(dblog, from_lsn=lsn(10))

    assert [frame["sale_id"].to_list() for frame in frames] == [[1, 2], [3]]
    assert dblog._dump_done is True


def test_a_dump_of_an_empty_table_still_drains_the_log(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales()]
    dblog._connector.event_script = [events(5)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300)]

    frames = full_run(dblog, from_lsn=lsn(10))

    assert [frame["sale_id"].to_list() for frame in frames] == [[5]]


def test_a_dump_run_stops_once_the_table_is_walked(factory_calls):
    """It does not go on tailing the log: the caller decides when to come back."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(3)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300), lsn(400)]

    full_run(dblog, from_lsn=lsn(10))

    assert dblog._connector.calls.count("read_event_log") == 2


def test_an_unnamed_run_records_nothing(factory_calls):
    dblog = dumping(factory_calls)
    dblog._connector.max_lsn = lsn(200)

    list(dblog.run("dbo", "sales", from_lsn=lsn(10)))

    assert dblog._store.saves == []


# ---------------------------------------------------------------------------
# Restarting — a dump is long enough that it will be interrupted
# ---------------------------------------------------------------------------


def test_records_progress_after_every_chunk(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(3)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300)]

    full_run(dblog, from_lsn=lsn(10))

    assert [(save.chunk_key, save.done) for save in dblog._store.saves] == [
        (3, False),
        (4, True),
    ]


def test_what_it_records_is_enough_to_resume_on(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2)]
    dblog._connector.max_lsn_script = [lsn(200)]

    full_run(dblog, from_lsn=lsn(10))

    first = dblog._store.saves[0]
    assert first.dump == "sales-backfill"
    assert first.table == "dbo.sales"
    assert first.last_lsn == lsn(201)
    assert first.chunk_key == 3


def test_records_progress_only_once_the_frames_are_taken(factory_calls):
    """
    Recording before the consumer has the frames would turn a crash into silent
    loss: the resume would skip a chunk nobody ever received.
    """
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1, 2), sales(3, 4)]
    dblog._connector.max_lsn_script = [lsn(200), lsn(300)]

    stream = dblog.run("dbo", "sales", dump="sales-backfill", from_lsn=lsn(10))
    next(stream)

    assert dblog._store.saves == []


def test_resumes_the_table_walk_where_it_stopped(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), chunk_key=77
    )
    dblog._connector.row_script = [sales(77, 78)]
    dblog._connector.max_lsn_script = [lsn(600)]

    full_run(dblog)

    assert dblog._connector.read_table_calls[0][1] == 77


def test_resumes_the_log_where_it_stopped(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), chunk_key=77
    )
    dblog._connector.row_script = [sales(77)]
    dblog._connector.max_lsn_script = [lsn(600)]

    full_run(dblog)

    assert dblog._connector.event_log_calls[0][1] == lsn(500)


def test_a_resume_ignores_the_lsn_the_caller_offered(factory_calls):
    """Recorded progress wins: the caller's guess would leave a gap or repeat one."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), chunk_key=77
    )
    dblog._connector.row_script = [sales(77)]
    dblog._connector.max_lsn_script = [lsn(600)]

    full_run(dblog, from_lsn=lsn(10))

    assert dblog._connector.event_log_calls[0][1] == lsn(500)


def test_refuses_to_resume_from_a_position_that_aged_out(factory_calls):
    """An interruption long enough to outlast retention cannot be resumed at all."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.min_lsn = lsn(900)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), chunk_key=77
    )

    with pytest.raises(CdcRetentionExpiredError, match="no longer retains"):
        full_run(dblog)


def test_refuses_a_dump_name_pointed_at_another_table(factory_calls):
    """Its chunk key is a position in a key space this table does not share."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.orders", last_lsn=lsn(500), chunk_key=77
    )

    with pytest.raises(ValueError, match="dbo.orders"):
        full_run(dblog)


def test_a_finished_dump_only_drains_the_log(factory_calls):
    """Re-running a completed dump is safe: the table is done, the log is not."""
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), done=True
    )
    dblog._connector.event_script = [events(9)]
    dblog._connector.max_lsn_script = [lsn(600), lsn(700)]

    frames = full_run(dblog)

    assert dblog._connector.read_table_calls == []
    assert [frame["sale_id"].to_list() for frame in frames] == [[9]]


def test_a_finished_dump_keeps_recording_where_the_log_reached(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._store.states["sales-backfill"] = DumpState(
        dump="sales-backfill", table="dbo.sales", last_lsn=lsn(500), done=True
    )
    dblog._connector.event_script = [events(9)]
    dblog._connector.max_lsn_script = [lsn(600), lsn(700)]

    full_run(dblog)

    assert dblog._store.saves[-1].last_lsn == lsn(601)
    assert dblog._store.saves[-1].done is True


def test_an_interrupted_dump_picks_up_from_its_last_record(factory_calls):
    """The whole point: a crash mid-dump costs one chunk, not the whole table."""
    store = StubStore()

    first = DBLog(**CONNECTION, chunk_size=2, state_store=store)
    first._connector.row_script = [sales(1, 2), sales(3, 4)]
    first._connector.max_lsn_script = [lsn(200), lsn(300)]
    stream = first.run("dbo", "sales", dump="sales-backfill", from_lsn=lsn(10))
    taken = [next(stream), next(stream)]  # one chunk through, then "crash"
    stream.close()

    second = DBLog(**CONNECTION, chunk_size=2, state_store=store)
    second._connector.row_script = [sales(3, 4), sales(5)]
    second._connector.max_lsn_script = [lsn(400), lsn(500)]
    resumed = full_run(second)

    assert taken[0]["sale_id"].to_list() == [1, 2]
    assert second._connector.read_table_calls[0][1] == 3
    assert [frame["sale_id"].to_list() for frame in resumed] == [[3, 4], [5]]


def test_a_dump_run_ends_at_the_handoff_position(factory_calls):
    dblog = dumping(factory_calls, chunk_size=2)
    dblog._connector.row_script = [sales(1)]
    dblog._connector.max_lsn = lsn(200)

    full_run(dblog, from_lsn=lsn(10))

    assert dblog.last_lsn == lsn(201)


# ---------------------------------------------------------------------------
# run(dump=False) — drain the log and stop; polling belongs to the caller
# ---------------------------------------------------------------------------


def test_yields_each_window_that_held_events(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog._connector.max_lsn_script = [lsn(100), lsn(200), lsn(300)]
    dblog._connector.event_script = [
        DataFrame({"sale_id": [1]}),
        DataFrame({"sale_id": [2]}),
    ]

    frames = drain(dblog, from_lsn=lsn(10))

    assert [frame.to_dicts() for frame in frames] == [
        [{"sale_id": 1}],
        [{"sale_id": 2}],
    ]


def test_stops_once_the_log_is_caught_up(factory_calls):
    """Three max LSNs are on offer; the run stops at the first empty window."""
    dblog = DBLog(**CONNECTION)
    dblog._connector.max_lsn_script = [lsn(100), lsn(200), lsn(300)]
    dblog._connector.event_script = [DataFrame({"sale_id": [1]})]

    drain(dblog, from_lsn=lsn(10))

    assert len(dblog._connector.event_log_calls) == 2


def test_yields_nothing_when_there_is_no_new_event(factory_calls):
    dblog = DBLog(**CONNECTION)
    dblog._connector.events = None

    assert drain(dblog, from_lsn=lsn(10)) == []


def test_leaves_the_last_lsn_at_the_handoff_position(factory_calls):
    """What the caller passes back as from_lsn on the next drain."""
    dblog = DBLog(**CONNECTION)
    dblog._connector.max_lsn = lsn(100)

    drain(dblog, from_lsn=lsn(10))

    assert dblog.last_lsn == lsn(101)


def test_run_does_nothing_until_it_is_iterated(factory_calls):
    """It is a generator: a run that is never consumed must not touch the source."""
    dblog = DBLog(**CONNECTION)

    dblog.run("dbo", "sales")

    assert dblog._connector.inspect_calls == []
