"""
DBLog end to end, against a real SQL Server.

``test_dblog.py`` proves the orchestration against a stub. This file proves the one
thing a stub cannot: that a whole run's frames actually stack. Every frame comes out of
a different result set — some out of the change table, some off the table itself — and
before the schema was pinned, nothing stopped two of them from disagreeing about a
decimal's scale or a null column's type. A consumer only found out at the concat, with
progress already checkpointed against the chunk that broke it.

There is nothing to build here but the connection: the run is a value threaded through
``dblog()``, so the two runs below share one connector and keep their own state.
"""

from typing import NamedTuple

import polars as pl
import pytest
from polars import DataFrame

from pydblog.connectors.mssql import LSN_WIDTH, OP_DUMP
from pydblog.dblog import dblog
from pydblog.state import RunState

from conftest import (
    LAB_SCHEMA,
    LAB_TABLE,
    execute,
    latest_change_lsn,
    scalar,
    wait_for_cdc,
)

# chunk_size is 2 against a table of more than that, so a run takes several chunks and
# several windows — which is the case that matters, since a single frame can never
# disagree with itself.
CHUNK_SIZE = 2


def walk(connector, chunk_size: int = CHUNK_SIZE) -> tuple[list[DataFrame], RunState]:
    """
    Every frame up to the end of the table, the way a backfill bounds its loop.

    Threads the state the way a caller must, since nothing else carries the position.
    """
    frames: list[DataFrame] = []
    carried: RunState | None = None

    while carried is None or not carried.dump_done:
        result = dblog(
            connector,
            schema=LAB_SCHEMA,
            table=LAB_TABLE,
            state=carried,
            chunk_size=chunk_size,
        )
        carried = result.state
        if result.frame is not None:
            frames.append(result.frame)

    return frames, carried


@pytest.fixture(scope="module")
def dumped(connector, spec) -> list[DataFrame]:
    """
    Every frame of a dump run, with writes landing while it walks the table.

    Module-scoped: one run answers every question below, and a dump run is not cheap.
    """
    frames: list[DataFrame] = []
    carried: RunState | None = None
    written = False

    while carried is None or not carried.dump_done:
        result = dblog(
            connector,
            schema=LAB_SCHEMA,
            table=LAB_TABLE,
            state=carried,
            chunk_size=CHUNK_SIZE,
        )
        carried = result.state
        if result.frame is not None:
            frames.append(result.frame)

        if not written:
            written = True
            # A write mid-run, so at least one window is non-empty and the merge path
            # is exercised rather than every chunk sailing through untouched.
            #
            # Waiting for the capture job is what makes that a fact rather than a
            # hope: it runs on its own schedule, and without the wait a short dump
            # finishes before the insert is ever captured, leaving a run of nothing
            # but dump rows.
            before = latest_change_lsn(connector, spec.capture_instance) or bytes(10)
            execute(
                connector,
                f"INSERT INTO {LAB_SCHEMA}.{LAB_TABLE} "
                "(product_id, customer_id, quantity, unit_price, status) "
                "VALUES (?, ?, ?, ?, ?)",
                [811, 812, 2, 7.25, "PENDING"],
            )
            wait_for_cdc(connector, spec.capture_instance, before)

    return frames


@pytest.mark.integration
def test_a_run_yields_more_than_one_frame(dumped):
    """Otherwise everything below is vacuously true."""
    assert len(dumped) > 1


@pytest.mark.integration
def test_every_frame_of_a_run_shares_one_schema(dumped):
    assert len({tuple(frame.schema.items()) for frame in dumped}) == 1


@pytest.mark.integration
def test_a_whole_run_concatenates_vertically(dumped):
    """
    The problem this change exists to solve: no how='diagonal', no relaxed cast, no
    reconciling two layouts. This raised before the schema was pinned.
    """
    assert pl.concat(dumped).height == sum(frame.height for frame in dumped)


@pytest.mark.integration
def test_a_run_carries_both_dump_rows_and_log_events(dumped):
    """A dump run interleaves the two, so the concat above spans both shapes."""
    operations = pl.concat(dumped)["operation"]

    assert operations.eq(OP_DUMP).any()
    assert operations.ne(OP_DUMP).any()


@pytest.mark.integration
def test_dump_rows_are_marked_with_an_all_zero_lsn(dumped):
    dump_rows = pl.concat(dumped).filter(pl.col("operation") == OP_DUMP)

    assert set(dump_rows["start_lsn"].to_list()) == {bytes(LSN_WIDTH)}


@pytest.mark.integration
def test_log_events_are_never_marked_as_dump_rows(dumped):
    """Zero is a position CDC never issues, which is what makes the mark unambiguous."""
    events = pl.concat(dumped).filter(pl.col("operation") != OP_DUMP)

    assert bytes(LSN_WIDTH) not in set(events["start_lsn"].to_list())


@pytest.mark.integration
def test_every_row_of_a_run_is_dated(dumped):
    """
    Every row, not just the first chunk's. Dating a chunk from the position where the
    previous window closed would leave all but the first with no time at all: that
    position is one past a real LSN, and CDC records no commit time for it.
    """
    assert pl.concat(dumped)["commit_timestamp"].null_count() == 0


@pytest.mark.integration
def test_dump_rows_are_dated_in_the_order_the_chunks_were_read(dumped):
    """
    Each chunk is dated from a watermark taken before it is read, and watermarks only
    move forward — so the frames come out non-decreasing. A chunk dated from anything
    other than its own pass would break the ordering without breaking anything else.
    """
    dated = [
        frame.filter(pl.col("operation") == OP_DUMP)["commit_timestamp"].max()
        for frame in dumped
        if frame.filter(pl.col("operation") == OP_DUMP).height > 0
    ]

    assert dated == sorted(dated)


# ---------------------------------------------------------------------------
# The handoff — a walked table's state reads the log and nothing else
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def handed_off(connector, spec) -> DataFrame:
    """
    A row written after a dump finished, read back through the dump's own final state.

    The reason the state comes back out of ``dblog()`` at all: there is no other way to
    open a log-only run exactly where the dump stopped, with nothing repeated and
    nothing skipped.
    """
    _, walked = walk(connector)
    assert walked.dump_done

    before = latest_change_lsn(connector, spec.capture_instance) or bytes(10)
    execute(
        connector,
        f"INSERT INTO {LAB_SCHEMA}.{LAB_TABLE} "
        "(product_id, customer_id, quantity, unit_price, status) "
        "VALUES (?, ?, ?, ?, ?)",
        [911, 912, 3, 1.5, "HANDED_OFF"],
    )
    wait_for_cdc(connector, spec.capture_instance, before)

    frames: list[DataFrame] = []
    carried = walked
    while True:
        result = dblog(
            connector, schema=LAB_SCHEMA, table=LAB_TABLE, state=carried
        )
        carried = result.state
        if result.frame is None:
            break
        frames.append(result.frame)

    return pl.concat(frames)


@pytest.mark.integration
def test_the_tail_carries_what_was_written_after_the_dump(handed_off):
    assert "HANDED_OFF" in handed_off["status"].to_list()


@pytest.mark.integration
def test_the_tail_reads_the_log_and_not_the_table(handed_off):
    """A finished dump has no table left to walk, so nothing may be marked as dumped."""
    assert handed_off.filter(pl.col("operation") == OP_DUMP).is_empty()


@pytest.mark.integration
def test_the_tail_frames_still_share_the_run_s_schema(handed_off, dumped):
    """The handoff is only useful if what comes after it stacks with what came before."""
    assert pl.concat([*dumped, handed_off]).height == (
        sum(frame.height for frame in dumped) + handed_off.height
    )


# ---------------------------------------------------------------------------
# The barrier — a write landing inside a chunk scan
# ---------------------------------------------------------------------------


class Raced(NamedTuple):
    """What a run did to one row that changed while its chunk was being scanned."""

    fired: bool
    stale: str
    rows: DataFrame


@pytest.fixture(scope="module")
def raced(connector, spec) -> Raced:
    """
    A dump run with a write landing inside a chunk scan.

    ``_next_chunk`` and ``_read_window`` are adjacent in the loop, so wrapping
    ``read_table`` drops the write exactly where the paper's high watermark has to
    cover: after the rows were read, before the window closes. Without the barrier this
    reproduced a stale row every time.
    """
    target = scalar(connector, f"SELECT MIN(sale_id) FROM {LAB_SCHEMA}.{LAB_TABLE}")
    stale = scalar(
        connector,
        f"SELECT status FROM {LAB_SCHEMA}.{LAB_TABLE} WHERE sale_id = ?",
        [target],
    )

    original = connector.read_table
    fired: list[int] = []

    def read_then_write(*args, **kwargs):
        rows = original(*args, **kwargs)
        if not fired and target in rows["sale_id"].to_list():
            execute(
                connector,
                f"UPDATE {LAB_SCHEMA}.{LAB_TABLE} SET status = ? WHERE sale_id = ?",
                ["RACED", target],
            )
            fired.append(target)
        return rows

    connector.read_table = read_then_write
    try:
        frames, _ = walk(connector)
    finally:
        connector.read_table = original

    return Raced(
        fired=bool(fired),
        stale=stale,
        rows=pl.concat(frames).filter(pl.col("sale_id") == target),
    )


@pytest.mark.integration
def test_the_race_actually_happened(raced):
    """Otherwise the assertions below prove nothing."""
    assert raced.fired


@pytest.mark.integration
def test_a_write_during_a_chunk_scan_does_not_escape_as_a_stale_row(raced):
    """
    The property the watermarks exist for. The chunk read the row before the update
    committed, so without the barrier it emitted the pre-update value and the run never
    corrected it.
    """
    dump_rows = raced.rows.filter(pl.col("operation") == OP_DUMP)

    assert raced.stale not in dump_rows["status"].to_list()


@pytest.mark.integration
def test_the_change_made_during_the_scan_is_in_the_run(raced):
    """Dropped from the chunk, so the log has to be the one carrying it."""
    assert "RACED" in raced.rows["status"].to_list()


@pytest.mark.integration
def test_a_computed_column_is_null_on_both_sides(dumped):
    """
    CDC records nothing for a computed column, so a dump row carries nothing either.
    The value differing by which side a row came from is the trap this avoids.
    """
    assert pl.concat(dumped)["total_amount"].null_count() == pl.concat(dumped).height
