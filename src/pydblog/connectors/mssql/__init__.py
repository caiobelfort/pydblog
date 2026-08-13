"""
The SQL Server source: the connector, and the schema every frame it reads is pinned to.

``MSSQLConnector`` is re-exported so callers name the package rather than the module
it happens to live in — ``build_connector`` imports it from here.
"""

from pydblog.connectors.mssql.connector import (
    LSN_WIDTH,
    OP_DELETE,
    OP_DUMP,
    OP_INSERT,
    OP_UPDATE_AFTER,
    OP_UPDATE_BEFORE,
    MSSQLConnector,
)

__all__ = [
    "LSN_WIDTH",
    "OP_DELETE",
    "OP_DUMP",
    "OP_INSERT",
    "OP_UPDATE_AFTER",
    "OP_UPDATE_BEFORE",
    "MSSQLConnector",
]
