"""
Dump state store tests.

The store is what makes a dump survive a crash, so what matters is that a state
written before an interruption reads back identically after one — including the LSN,
which is raw bytes that JSON cannot hold directly.
"""

import json
from typing import Any

import pytest

from pydblog.state import DumpState, JsonFileStore


def state(**overrides) -> DumpState:
    # Annotated because the values are of mixed type: without it the `|` below
    # widens every value to their union and none of them fits its own field.
    fields: dict[str, Any] = {
        "dump": "sales-backfill",
        "table": "dbo.sales",
        "last_lsn": bytes.fromhex("0000004400007920000b"),
        "chunk_key": 42,
        "done": False,
    }
    return DumpState(**(fields | overrides))


@pytest.fixture
def store(tmp_path) -> JsonFileStore:
    return JsonFileStore(tmp_path / "state")


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_reads_back_what_it_wrote(store):
    store.save(state())
    assert store.load("sales-backfill") == state()


def test_the_lsn_survives_as_bytes(store):
    """Hex on disk, bytes in the API: JSON has no way to hold the raw value."""
    store.save(state())

    loaded = store.load("sales-backfill")

    assert loaded.last_lsn == bytes.fromhex("0000004400007920000b")
    assert isinstance(loaded.last_lsn, bytes)


def test_writes_the_lsn_as_hex_on_disk(store):
    store.save(state())

    written = json.loads((store.directory / "sales-backfill.json").read_text())

    assert written["last_lsn"] == "0000004400007920000b"


def test_a_dump_that_never_ran_has_no_state(store):
    assert store.load("never-ran") is None


def test_a_finished_dump_round_trips_its_done_flag(store):
    store.save(state(done=True, chunk_key=None))

    loaded = store.load("sales-backfill")

    assert loaded.done is True
    assert loaded.chunk_key is None


def test_saving_again_replaces_the_previous_state(store):
    store.save(state(chunk_key=42))
    store.save(state(chunk_key=99))

    assert store.load("sales-backfill").chunk_key == 99


def test_dumps_do_not_see_each_other(store):
    store.save(state(dump="one", chunk_key=1))
    store.save(state(dump="two", chunk_key=2))

    assert store.load("one").chunk_key == 1
    assert store.load("two").chunk_key == 2


def test_makes_its_directory_on_demand(tmp_path):
    store = JsonFileStore(tmp_path / "does" / "not" / "exist")

    store.save(state())

    assert store.load("sales-backfill") == state()


# ---------------------------------------------------------------------------
# Clearing
# ---------------------------------------------------------------------------


def test_clearing_forgets_the_dump(store):
    store.save(state())

    store.clear("sales-backfill")

    assert store.load("sales-backfill") is None


def test_clearing_a_dump_that_never_ran_is_not_an_error(store):
    store.clear("never-ran")


# ---------------------------------------------------------------------------
# Naming — a dump name is caller input, and it becomes a filename
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["../escape", "dbo/sales", "..", "with space", "a:b", "café"]
)
def test_a_dump_name_stays_inside_the_directory(store, name):
    store.save(state(dump=name))

    written = list(store.directory.iterdir())

    assert len(written) == 1
    assert written[0].parent == store.directory
    assert store.load(name).dump == name


def test_names_that_differ_only_by_escaping_stay_apart(store):
    store.save(state(dump="a/b", chunk_key=1))
    store.save(state(dump="a%2Fb", chunk_key=2))

    assert store.load("a/b").chunk_key == 1
    assert store.load("a%2Fb").chunk_key == 2


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_a_save_leaves_no_temporary_file_behind(store):
    store.save(state())

    assert [path.name for path in store.directory.iterdir()] == [
        "sales-backfill.json"
    ]


def test_the_state_file_is_never_seen_half_written(store, monkeypatch):
    """
    The whole point of the store is surviving a crash, so the swap into place has
    to be atomic. If the write is interrupted, the previous state must still stand.
    """
    store.save(state(chunk_key=42))

    def die(*args, **kwargs):
        raise OSError("crash mid-write")

    monkeypatch.setattr("pydblog.state.os.replace", die)

    with pytest.raises(OSError):
        store.save(state(chunk_key=99))

    assert store.load("sales-backfill").chunk_key == 42
