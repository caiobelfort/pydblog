"""
Where a dump's progress is kept between runs.

A dump is long enough that it will be interrupted — a dropped connection, a restart,
a crash. What makes it restartable is remembering two things together: how far
through the table it got, and where in the change log it was at that moment. Either
one alone is useless, so they are written as a unit.

``DBLog`` holds a ``StateStore``, not a file, so where that unit lands is a decision
the caller makes rather than one the algorithm bakes in.
"""

import json
import os
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

from pydantic import BaseModel, field_serializer, field_validator

from pydblog.connectors.types import LSN

DEFAULT_STATE_DIRECTORY = Path(".pydblog-state")


class DumpState(BaseModel):
    """
    How far a named dump got.

    Attributes:
        dump: The dump's name, which is what it is looked up by.
        table: Qualified name of the table being dumped. A dump name pointed at a
            different table is a mistake, not a resume: the chunk key would be a
            position in a key space that table does not share.
        last_lsn: Where the change log had been read to.
        chunk_key: Leading primary key the next chunk starts at, or None before the
            first chunk.
        done: Whether the table has been walked to the end.
    """

    dump: str
    table: str
    last_lsn: LSN
    chunk_key: int | None = None
    done: bool = False

    @field_serializer("last_lsn")
    def _lsn_to_hex(self, value: LSN) -> str:
        return value.hex()

    @field_validator("last_lsn", mode="before")
    @classmethod
    def _lsn_from_hex(cls, value: object) -> object:
        return bytes.fromhex(value) if isinstance(value, str) else value


class StateStore(Protocol):
    """Keeps the progress of named dumps."""

    def load(self, dump: str) -> DumpState | None:
        """
        Fetch what a dump recorded, or None if it has never run.
        """
        ...

    def save(self, state: DumpState) -> None:
        """
        Record a dump's progress, replacing whatever it recorded before.

        A reader must never see a partial state: either the previous one stands or
        the new one does.
        """
        ...

    def clear(self, dump: str) -> None:
        """
        Forget a dump, so running it again starts from the beginning.

        Forgetting one that never ran is not an error.
        """
        ...


class JsonFileStore:
    """
    Keeps each dump's progress in its own JSON file.

    Writes land on a temporary file that is then moved into place, so an interrupted
    write leaves the previous state intact rather than a truncated one.
    """

    def __init__(self, directory: Path | str = DEFAULT_STATE_DIRECTORY) -> None:
        """
        Args:
            directory: Where the files go. Created when the first one is written.
        """

        self.directory = Path(directory)

    def _path(self, dump: str) -> Path:
        # The name comes from the caller and becomes a filename, so a dump called
        # "../x" must not write outside the directory. Percent-encoding keeps
        # ordinary names readable and makes distinct names distinct files.
        return self.directory / f"{quote(dump, safe='')}.json"

    def load(self, dump: str) -> DumpState | None:
        """
        Fetch what a dump recorded, or None if it has never run.

        Args:
            dump: Name of the dump.

        Returns:
            Its recorded progress, or None.
        """

        path = self._path(dump)
        if not path.exists():
            return None

        return DumpState.model_validate_json(path.read_text(encoding="utf-8"))

    def save(self, state: DumpState) -> None:
        """
        Record a dump's progress, replacing whatever it recorded before.

        Args:
            state: The progress to record.
        """

        self.directory.mkdir(parents=True, exist_ok=True)

        path = self._path(state.dump)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(state.model_dump_json(indent=2), encoding="utf-8")

        try:
            os.replace(temporary, path)
        except OSError:
            temporary.unlink(missing_ok=True)
            raise

    def clear(self, dump: str) -> None:
        """
        Forget a dump, so running it again starts from the beginning.

        Args:
            dump: Name of the dump.
        """

        self._path(dump).unlink(missing_ok=True)
