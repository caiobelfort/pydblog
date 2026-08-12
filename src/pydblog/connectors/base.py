
from datetime import datetime
from pydblog.connectors.types import SourceType
from pydblog.connectors.types import LSN
from pydblog.connectors.types import TableSpec
from typing import Protocol
from polars import DataFrame



class SourceConnector(Protocol):

    def get_max_lsn(self) -> LSN:
        "Fetch the highest Log Sequence Number in the database"
        ...

    def get_min_lsn(self, capture_instance: str) -> LSN:
        "Fetch the lowest Log Sequence Number available for a table's events"
        ...

    def connect(self) -> None:
        "Open a connection to the database"
        ...

    def close(self) -> None:
        "Close a connection to the database"
        ...

    def inspect(self, schema: str, table: str) -> TableSpec:
        """
        Inspect a table's schema and return the metadata that matters about it.
        """
        ...

    def read_event_log(self, spec: TableSpec, from_lsn: LSN, to_lsn: LSN) -> DataFrame | None:
        """
        Read change data capture events for a table in the database.

        Returns None when there is no event at all in the LSN range.
        """
        ...

    def read_table(
        self,
        spec: TableSpec,
        start_pk: int | None = None,
        end_pk: int | None = None,
        limit: int = 0,
    ) -> DataFrame:
        """
        Read the half-open key range [start_pk, end_pk) from the table.

        The bounds apply to the leading primary key column; None on either side means
        unbounded. Rows come back ordered by the full primary key.

        Pagination is by key value rather than by position, so a chunk plan computed
        once stays valid under concurrent writes.

        The limit is an optional cap on rows read; 0 means uncapped.
        """
        ...

    def read_pk_range(self, spec: TableSpec) -> tuple[int, int] | None:
        """
        Read the lowest and highest value of the leading primary key column.

        Returns None when the table is empty. This is what the chunk plan is sliced
        out of, so the leading key column has to be an integer type.
        """
        ...

    def map_lsn_to_timestamp(self, lsn: LSN) -> datetime | None:
        """
        Read the commit time recorded against an LSN.

        Returns None when the LSN falls outside the range the log tracks, which is a
        real answer rather than a failure: an LSN either has a recorded commit time
        or it does not.
        """
        ...

    def increment_lsn(self, lsn: LSN) -> LSN:
        """
        Return the next LSN after the given one.

        Needed because the CDC read is inclusive on both bounds, so consecutive windows
        have to start one LSN past where the previous one ended.
        """
        ...


def build_connector(
    source_type: SourceType | str,
    host: str,
    port: str,
    user: str,
    password: str,
    database: str,
    *args,
    **kwargs
) -> SourceConnector:
    """
    Instantiate a connector
    """

    source_type = SourceType(source_type)
    match source_type:
        case SourceType.MSSQL:
            from pydblog.connectors.mssql import MSSQLConnector

            return MSSQLConnector(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                *args,
                **kwargs
            )
        case _ :
            raise Exception(f"Source of type not defined")
