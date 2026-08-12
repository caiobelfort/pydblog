

from pydantic import BaseModel
from enum import StrEnum


class SourceType(StrEnum):
    MSSQL = 'mssql'
    POSTGRES = 'postgres'


class TableSpec(BaseModel):
    source_schema: str
    source_table: str
    pk_columns: list[str]
    business_columns: list[str]
    capture_instance: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.source_schema}.{self.source_table}"


type LSN = bytes
