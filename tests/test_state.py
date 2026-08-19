"""
Run state tests.

The state is what a caller writes down between calls, so what matters is that one
written before an interruption reads back identically after one — including the LSN,
which is raw bytes that JSON cannot hold directly, and the table spec, which is what
settles the schema every frame of a run shares.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from polars import DataFrame
from pydantic import ValidationError

from pydblog.connectors.types import ColumnSpec, TableSpec
from pydblog.state import BatchResult, RunState

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

INSPECTED = datetime(2026, 8, 19, 12, 30, tzinfo=UTC)


def state(**overrides) -> RunState:
    # Annotated because the values are of mixed type: without it the `|` below widens
    # every value to their union and none of them fits its own field.
    fields: dict[str, Any] = {
        "spec": SPEC,
        "last_lsn": bytes.fromhex("0000004400007920000b"),
        "last_inspect": INSPECTED,
        "chunk_key": 42,
        "dump_done": False,
    }
    return RunState(**(fields | overrides))


def written(carried: RunState) -> dict:
    """What the caller would have on disk."""
    return json.loads(carried.model_dump_json())


# ---------------------------------------------------------------------------
# Round trip — the state has to survive being written down
# ---------------------------------------------------------------------------


def test_reads_back_what_it_wrote():
    assert RunState.model_validate_json(state().model_dump_json()) == state()


def test_the_lsn_survives_as_bytes():
    """Hex on the way out, bytes in the API: JSON has no way to hold the raw value."""
    restored = RunState.model_validate_json(state().model_dump_json())

    assert restored.last_lsn == bytes.fromhex("0000004400007920000b")
    assert isinstance(restored.last_lsn, bytes)


def test_writes_the_lsn_as_hex():
    assert written(state())["last_lsn"] == "0000004400007920000b"


def test_the_spec_survives_the_round_trip():
    """It settles the schema of every frame, so a run resumed on a lost spec would be a
    run whose frames stopped stacking."""
    restored = RunState.model_validate_json(state().model_dump_json())

    assert restored.spec == SPEC
    assert restored.spec.capture_instance == "dbo_sales"
    assert restored.spec.business_columns == ["sale_id", "amount"]


def test_the_chunk_key_survives_the_round_trip():
    restored = RunState.model_validate_json(state(chunk_key=7).model_dump_json())

    assert restored.chunk_key == 7


def test_a_state_before_the_first_chunk_survives_the_round_trip():
    restored = RunState.model_validate_json(state(chunk_key=None).model_dump_json())

    assert restored.chunk_key is None


def test_a_finished_dump_survives_the_round_trip():
    """This is the flag that turns a walked table into a tail, so losing it would put a
    finished dump back to the start of the table."""
    restored = RunState.model_validate_json(state(dump_done=True).model_dump_json())

    assert restored.dump_done is True


def test_the_inspection_time_survives_the_round_trip():
    restored = RunState.model_validate_json(state().model_dump_json())

    assert restored.last_inspect == INSPECTED
    assert restored.last_inspect.tzinfo is not None


# ---------------------------------------------------------------------------
# The inspection time — it is compared against an aware now
# ---------------------------------------------------------------------------


def test_a_naive_time_is_read_as_utc():
    """Subtracting a naive datetime from an aware one raises rather than comparing, and
    a caller who built the state by hand should not find that out a day later."""
    carried = state(last_inspect=datetime(2026, 8, 19, 12, 30))

    assert carried.last_inspect == INSPECTED


def test_an_aware_time_is_left_alone():
    other = datetime(2026, 8, 19, 9, 30, tzinfo=UTC) - timedelta(hours=3)

    assert state(last_inspect=other).last_inspect == other


# ---------------------------------------------------------------------------
# Immutability — a threaded value that could be changed in place is not one
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field, value",
    [
        ("last_lsn", bytes(10)),
        ("chunk_key", 99),
        ("dump_done", True),
        ("spec", SPEC),
    ],
)
def test_the_state_cannot_be_changed_in_place(field, value):
    with pytest.raises(ValidationError):
        setattr(state(), field, value)


def test_advancing_a_state_leaves_the_original_alone():
    """How the algorithm advances one: a copy, so the caller's own is still theirs."""
    carried = state(chunk_key=42)

    advanced = carried.model_copy(update={"chunk_key": 43})

    assert carried.chunk_key == 42
    assert advanced.chunk_key == 43


# ---------------------------------------------------------------------------
# BatchResult — the frame and where to carry on from, together
# ---------------------------------------------------------------------------


def test_a_result_carries_the_frame_and_the_state():
    frame = DataFrame({"sale_id": [1]})

    result = BatchResult(frame=frame, state=state())

    assert result.frame is frame
    assert result.state == state()


def test_a_result_carries_a_state_even_with_no_frame():
    """A window that held nothing still moved the log position, so there is always a
    state to keep."""
    result = BatchResult(frame=None, state=state())

    assert result.frame is None
    assert result.state == state()


def test_a_result_cannot_be_changed_in_place():
    result = BatchResult(frame=None, state=state())

    with pytest.raises(Exception):
        result.state = state(chunk_key=99)  # type: ignore[misc]
