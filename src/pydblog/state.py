"""
What a run carries between calls, and what one call hands back.

A run is long enough that it will be interrupted — a dropped connection, a restart, a
crash. What makes it restartable is remembering two things together: how far through the
table it got, and where in the change log it was at that moment. Either one alone is
useless, so they travel as a unit in a single frozen value the caller cannot split.

``dblog()`` neither reads nor writes that value anywhere. It takes the state it was
given and returns the state the call reached, so where it is kept — a file, a row, the
same transaction as the data itself — is the caller's decision rather than one the
algorithm bakes in. It also means a position cannot get ahead of the data it describes:
the frame and the state come back together, and the caller writes both or neither.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from polars import DataFrame
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

from pydblog.connectors.types import LSN, TableSpec


class RunState(BaseModel):
    """
    Everything a run needs to pick up where the last call left off.

    Frozen, and every call returns a new one: a value threaded through calls that
    could be mutated in place would be a contract in name only.

    Attributes:
        spec: The table as ``inspect()`` described it. Carried rather than re-read
            because it is what settles the schema every frame of the run shares, and
            a spec re-read per call would be a round trip per batch. Refreshed on the
            interval ``dblog()`` is given.
        last_lsn: Where the next log window opens. One past the high bound of the last
            window read, since the CDC read is inclusive on both ends — so it is not a
            position the log ever issued.
        last_inspect: When ``spec`` was read, so the next call knows whether it is
            stale. Timezone-aware; a naive value is read as UTC.
        chunk_key: Leading primary key the next chunk starts at, or None before the
            first chunk.
        dump_done: Whether the table has been walked to the end. Once it is True the
            state reads windows alone, which is how a finished dump becomes a tail.
    """

    model_config = ConfigDict(frozen=True)

    spec: TableSpec
    last_lsn: LSN
    last_inspect: datetime
    chunk_key: int | None = None
    dump_done: bool = False

    @field_serializer("last_lsn")
    def _lsn_to_hex(self, value: LSN) -> str:
        return value.hex()

    @field_validator("last_lsn", mode="before")
    @classmethod
    def _lsn_from_hex(cls, value: object) -> object:
        return bytes.fromhex(value) if isinstance(value, str) else value

    @field_validator("last_inspect")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        # The interval check subtracts this from the current time, and mixing an aware
        # and a naive datetime raises rather than comparing. A caller who built the
        # state by hand gets read as UTC instead of a TypeError one day later.
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


@dataclass(frozen=True, eq=False)
class BatchResult:
    """
    What one call to ``dblog()`` produced: a frame, and where to carry on from.

    One object rather than a pair, because the second half is not optional. A caller
    who drops it reads the same batch forever, and the state has to be kept even off
    the result whose frame is None — a window advances ``last_lsn`` even when it held
    nothing, so that an idle table does not re-scan an ever-widening range.

    A dataclass rather than a model: it holds a polars frame, which no serialization
    of the state should ever try to carry. ``eq=False`` because comparing two frames
    yields a frame rather than a bool.

    Attributes:
        frame: The batch, or None when there was nothing to read — the table is walked
            and the log is caught up as of this call. Not permanent: a later call picks
            up whatever has landed since, so it ends a loop rather than the run.
        state: Where the next call should carry on from.
    """

    frame: DataFrame | None
    state: RunState
