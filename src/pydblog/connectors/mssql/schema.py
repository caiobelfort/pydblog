"""
The schema every frame read out of SQL Server is pinned to.

``mssql_python`` returns a ``pyarrow.Table``, and polars takes its dtypes from that
table's schema. Arrow is therefore where a schema can actually be decided rather than
observed: casting there settles the dtype before polars ever sees the data, whereas
correcting it afterwards means letting the driver's per-result-set inference land
first. That inference is the problem — it is free to widen a decimal, pick a time
unit, or type an all-null column differently from one chunk to the next, and a
consumer stacking those chunks finds out at the join.

The map is a pure function of the metadata ``inspect()`` reads, so every frame of a
run conforms to one schema regardless of what any single result set contained.
"""

import pyarrow as pa

from pydblog.connectors.types import ColumnSpec, TableSpec

# The change log's own columns, which every event frame carries ahead of the table's.
# Dump rows are stamped with the same five so both halves of a run share one schema.
METADATA_FIELDS = [
    pa.field("start_lsn", pa.large_binary()),
    pa.field("seqval", pa.large_binary()),
    pa.field("operation", pa.int32()),
    pa.field("update_mask", pa.large_binary()),
    pa.field("commit_timestamp", pa.timestamp("us")),
]

METADATA_COLUMNS = [field.name for field in METADATA_FIELDS]

# Fixed-width types, where the SQL Server name settles the Arrow type on its own.
_FIXED: dict[str, pa.DataType] = {
    "bigint": pa.int64(),
    "int": pa.int32(),
    "smallint": pa.int16(),
    "tinyint": pa.uint8(),
    "bit": pa.bool_(),
    "float": pa.float64(),
    "real": pa.float32(),
    "date": pa.date32(),
    # Large variants throughout: SQL Server's max-length types have no 2 GB ceiling to
    # respect, and polars reads both the same way.
    "char": pa.large_string(),
    "varchar": pa.large_string(),
    "nchar": pa.large_string(),
    "nvarchar": pa.large_string(),
    "text": pa.large_string(),
    "ntext": pa.large_string(),
    "xml": pa.large_string(),
    "sysname": pa.large_string(),
    "uniqueidentifier": pa.large_string(),
    "binary": pa.large_binary(),
    "varbinary": pa.large_binary(),
    "image": pa.large_binary(),
    "timestamp": pa.large_binary(),
    "rowversion": pa.large_binary(),
}

# Types whose Arrow form is built from the precision and scale sys.types reports.
_DECIMALS = {"decimal", "numeric", "money", "smallmoney"}
_TIMESTAMPS = {"datetime", "smalldatetime", "datetime2"}


def _time_unit(scale: int) -> str:
    """The Arrow time unit that holds ``scale`` fractional digits without truncating.

    Bare ``datetime2`` and ``time`` default to scale 7, which microseconds cannot
    hold — so the unit follows the metadata rather than being fixed, and a table
    carries at most the two units its columns actually need.
    """
    return "ns" if scale >= 7 else "us"


def arrow_type(column: ColumnSpec) -> pa.DataType:
    """Map a SQL Server column to the Arrow type it is read as.

    Args:
        column: A column as ``inspect()`` described it.

    Returns:
        The Arrow type every read of that column is cast to.

    Raises:
        ValueError: If the type has no Arrow equivalent, which is caught at
            ``inspect()`` rather than partway through a run.
    """
    type_name = column.type_name.lower()

    if type_name in _FIXED:
        return _FIXED[type_name]

    if type_name in _DECIMALS:
        return pa.decimal128(column.precision, column.scale)

    if type_name in _TIMESTAMPS:
        return pa.timestamp(_time_unit(column.scale))

    if type_name == "datetimeoffset":
        # Normalised to UTC rather than kept at its stored offset: the offset varies
        # per row, and a column cannot carry more than one time zone.
        return pa.timestamp(_time_unit(column.scale), "UTC")

    if type_name == "time":
        return pa.time64(_time_unit(column.scale))

    raise ValueError(
        f"column {column.name!r} has type {column.type_name!r}, which has no Arrow "
        f"equivalent; it cannot be read into a frame with a stable schema"
    )


def _describe(column: ColumnSpec) -> str:
    """A column's type as the error messages name it."""
    type_name, precision, scale = column.signature

    return f"{type_name}({precision},{scale})"


def validate(spec: TableSpec) -> None:
    """Check that a spec can produce one schema for both of its read paths.

    Everything here is a condition under which the table read and the change log read
    would disagree, and every one of them is cheaper to hit at ``inspect()`` than
    partway through a dump, with progress already checkpointed against the chunk that
    is about to fail.

    Args:
        spec: Table metadata, with both column lists filled in.

    Raises:
        ValueError: If a captured column is gone from the source or has drifted from
            it, if a primary key column is not captured, if a business column is named
            like one of the log's own, or if a type has no Arrow equivalent.
    """
    source = {column.name: column for column in spec.columns}

    for captured in spec.captured_columns:
        origin = source.get(captured.name)

        if origin is None:
            raise ValueError(
                f"captured column {captured.name!r} is no longer in "
                f"{spec.qualified_name}: the change log carries a column the table "
                f"read cannot project"
            )

        if origin != captured:
            raise ValueError(
                f"captured column {captured.name!r} has drifted: the change log "
                f"records it as {_describe(captured)} and {spec.qualified_name} now "
                f"has {_describe(origin)}. CDC keeps the type it captured, so the two "
                f"reads would return different schemas for the same column."
            )

    business = spec.business_columns

    computed = {column.name for column in spec.captured_columns if column.is_computed}

    for key in spec.pk_columns:
        if key not in business:
            raise ValueError(
                f"primary key column {key!r} of {spec.qualified_name} is not captured: "
                f"chunk rows are matched against log events on the whole key, and an "
                f"uncaptured one leaves nothing to match on"
            )

        if key in computed:
            raise ValueError(
                f"primary key column {key!r} of {spec.qualified_name} is computed, so "
                f"it reads back null: there would be no key to page the table by and "
                f"nothing to match a chunk row against an event with"
            )

    for name in business:
        if name in METADATA_COLUMNS:
            raise ValueError(
                f"business column {name!r} of {spec.qualified_name} is named after one "
                f"of the change log's own columns; both would land in the same frame "
                f"and polars would reject it without saying why"
            )

    for column in spec.captured_columns:
        arrow_type(column)


def _fields(columns: list[ColumnSpec]) -> list[pa.Field]:
    """Build the Arrow fields for ``columns``, all nullable.

    Nullability is not tracked: CDC change tables make every captured column nullable
    whatever the source table says, so honouring a source ``NOT NULL`` would give one
    column two schemas across the two read paths. polars discards it on the way in
    regardless.
    """
    return [pa.field(column.name, arrow_type(column)) for column in columns]


def row_schema(spec: TableSpec) -> pa.Schema:
    """The schema a table read conforms to."""
    return pa.schema(_fields(spec.captured_columns))


def event_schema(spec: TableSpec) -> pa.Schema:
    """The schema a change log read conforms to, and dump rows are stamped into."""
    return pa.schema(METADATA_FIELDS + _fields(spec.captured_columns))


def conform(table: pa.Table, schema: pa.Schema, context: str) -> pa.Table:
    """Reorder and cast a result set to the schema it is supposed to have.

    Args:
        table: What the driver returned.
        schema: The declared schema, from ``row_schema`` or ``event_schema``.
        context: What is being read, for the error messages.

    Returns:
        The same rows, in the schema's column order and types.

    Raises:
        ValueError: If the column sets differ, if a name repeats, or if the values
            will not cast. pyarrow raises on the last of those already, but its
            message names neither the table nor the column.
    """
    names = table.column_names

    if len(names) != len(set(names)):
        repeated = sorted({name for name in names if names.count(name) > 1})
        raise ValueError(f"{context}: column name repeats in the result set: {repeated}")

    missing = [name for name in schema.names if name not in names]
    unexpected = [name for name in names if name not in schema.names]

    if missing or unexpected:
        raise ValueError(
            f"{context}: result set does not match the declared schema"
            + "".join(f"; missing column {name!r}" for name in missing)
            + "".join(f"; unexpected column {name!r}" for name in unexpected)
        )

    ordered = table.select(schema.names)

    return pa.table(
        [_cast(ordered.column(field.name), field, context) for field in schema],
        schema=schema,
    )


def _cast(column: pa.ChunkedArray, field: pa.Field, context: str) -> pa.ChunkedArray:
    """Cast one column, naming it if the cast fails."""
    try:
        return column.cast(field.type)
    except pa.ArrowException as exc:
        raise ValueError(
            f"{context}: column {field.name!r} came back as {column.type} and does not "
            f"cast to the declared {field.type}"
        ) from exc
