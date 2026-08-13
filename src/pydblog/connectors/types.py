

from pydantic import BaseModel, ConfigDict
from enum import StrEnum


class SourceType(StrEnum):
    MSSQL = 'mssql'
    POSTGRES = 'postgres'


class ColumnSpec(BaseModel):
    """A column as the source database describes it.

    Types are kept in the source's own vocabulary rather than translated on the way
    in, so the mapping to Arrow stays in one place and a second source type can
    describe its columns without borrowing SQL Server's names for them.
    """

    name: str
    type_name: str
    precision: int
    scale: int
    computed_definition: str | None = None

    @property
    def signature(self) -> tuple[str, int, int]:
        """What decides the type a column reads back as."""
        return self.type_name.lower(), self.precision, self.scale

    @property
    def is_computed(self) -> bool:
        """Whether the source derives this column rather than storing it.

        Carrying the formula rather than a flag is what lets a consumer recompute the
        value: the change log never records one, and neither does a dump row, so the
        column arrives null and the expression is all there is to go on.
        """
        return self.computed_definition is not None

    def __eq__(self, other: object) -> bool:
        """Two columns are equal when they name and read back as the same thing.

        Every field but ``is_computed``, which is the one the source table and the
        change log legitimately disagree on: a computed column is a plain column in
        the change table. Nothing else about a column can differ between the two
        without the reads returning incompatible frames.
        """
        if not isinstance(other, ColumnSpec):
            return NotImplemented

        return (self.name, self.signature) == (other.name, other.signature)

    def __hash__(self) -> int:
        return hash((self.name, self.signature))


class TableSpec(BaseModel):
    """What a table looks like to a read, settled once by ``inspect()``.

    Frozen: every read of a run is driven from the same spec, and a spec that changed
    partway would mean two chunks of one dump projecting different columns.
    """

    model_config = ConfigDict(frozen=True)

    source_schema: str
    source_table: str
    pk_columns: list[str]
    columns: list[ColumnSpec]
    captured_columns: list[ColumnSpec]
    capture_instance: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def business_columns(self) -> list[str]:
        """The columns every read projects, in the order the change log records them.

        Derived from ``captured_columns`` rather than from ``columns``: the change
        table is what both reads have to agree with, and a column the source has but
        the log does not is one the two paths could never produce alike.
        """
        return [column.name for column in self.captured_columns]


type LSN = bytes
