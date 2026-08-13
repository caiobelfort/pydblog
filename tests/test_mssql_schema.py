"""
Tests for the SQL Server -> Arrow type map.

No database. The map is a pure function of the column metadata ``inspect()`` reads,
which is what makes it worth pinning here rather than only against a live server.

The polars column in ``TYPE_MAP`` is the point of the whole exercise: the driver
hands back an Arrow table, so casting the Arrow schema is what decides the polars
dtype. Each case asserts both ends of that hop.
"""

from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pytest
from polars import (
    Binary,
    Boolean,
    DataFrame,
    Date,
    Datetime,
    Decimal as PlDecimal,
    Float32,
    Float64,
    Int16,
    Int32,
    Int64,
    String,
    Time,
    UInt8,
)
from polars.datatypes import DataType

from pydblog.connectors.mssql.schema import (
    METADATA_COLUMNS,
    arrow_type,
    conform,
    event_schema,
    row_schema,
    validate,
)
from pydblog.connectors.types import ColumnSpec, TableSpec

# (type_name, precision, scale) -> the Arrow type it maps to, and the polars dtype
# that Arrow type lands on once polars reads the table.
TYPE_MAP: list[tuple[str, int, int, pa.DataType, DataType]] = [
    ("bigint", 19, 0, pa.int64(), Int64),
    ("int", 10, 0, pa.int32(), Int32),
    ("smallint", 5, 0, pa.int16(), Int16),
    ("tinyint", 3, 0, pa.uint8(), UInt8),
    ("bit", 1, 0, pa.bool_(), Boolean),
    ("decimal", 10, 2, pa.decimal128(10, 2), PlDecimal(10, 2)),
    ("numeric", 38, 10, pa.decimal128(38, 10), PlDecimal(38, 10)),
    ("money", 19, 4, pa.decimal128(19, 4), PlDecimal(19, 4)),
    ("smallmoney", 10, 4, pa.decimal128(10, 4), PlDecimal(10, 4)),
    ("float", 53, 0, pa.float64(), Float64),
    ("real", 24, 0, pa.float32(), Float32),
    ("varchar", 0, 0, pa.large_string(), String),
    ("nvarchar", 0, 0, pa.large_string(), String),
    ("char", 0, 0, pa.large_string(), String),
    ("text", 0, 0, pa.large_string(), String),
    ("xml", 0, 0, pa.large_string(), String),
    ("uniqueidentifier", 0, 0, pa.large_string(), String),
    ("varbinary", 0, 0, pa.large_binary(), Binary),
    ("binary", 0, 0, pa.large_binary(), Binary),
    ("image", 0, 0, pa.large_binary(), Binary),
    ("timestamp", 0, 0, pa.large_binary(), Binary),
    ("date", 10, 0, pa.date32(), Date),
    ("time", 16, 7, pa.time64("ns"), Time),
    ("time", 16, 3, pa.time64("us"), Time),
    ("datetime", 23, 3, pa.timestamp("us"), Datetime("us")),
    ("smalldatetime", 16, 0, pa.timestamp("us"), Datetime("us")),
    ("datetime2", 27, 6, pa.timestamp("us"), Datetime("us")),
    ("datetime2", 27, 7, pa.timestamp("ns"), Datetime("ns")),
    ("datetimeoffset", 34, 7, pa.timestamp("ns", "UTC"), Datetime("ns", "UTC")),
]


def column(
    name: str = "c", type_name: str = "int", precision: int = 10, scale: int = 0
) -> ColumnSpec:
    return ColumnSpec(name=name, type_name=type_name, precision=precision, scale=scale)


def spec(*columns: ColumnSpec, captured: list[ColumnSpec] | None = None) -> TableSpec:
    return TableSpec(
        source_schema="dbo",
        source_table="sales",
        pk_columns=["sale_id"],
        columns=list(columns),
        captured_columns=list(columns) if captured is None else captured,
        capture_instance="dbo_sales",
    )


@pytest.mark.parametrize(
    ("type_name", "precision", "scale", "expected_arrow", "expected_polars"),
    TYPE_MAP,
    ids=[f"{name}({precision},{scale})" for name, precision, scale, _, _ in TYPE_MAP],
)
def test_arrow_type_maps_and_lands_on_the_expected_polars_dtype(
    type_name: str,
    precision: int,
    scale: int,
    expected_arrow: pa.DataType,
    expected_polars: DataType,
) -> None:
    mapped = arrow_type(column("value", type_name, precision, scale))
    landed = DataFrame(pa.schema([pa.field("value", mapped)]).empty_table())

    assert mapped == expected_arrow
    assert landed.schema["value"] == expected_polars


@pytest.mark.parametrize(
    "type_name", ["sql_variant", "geography", "geometry", "hierarchyid", "sysname_udt"]
)
def test_arrow_type_rejects_a_type_it_cannot_map(type_name: str) -> None:
    with pytest.raises(ValueError, match=rf"note_text.*{type_name}"):
        arrow_type(column("note_text", type_name))


def test_type_names_are_matched_case_insensitively() -> None:
    """sys.types is collation-dependent, so the case it reports is not guaranteed."""
    assert arrow_type(column("value", "BigInt")) == pa.int64()


def test_row_schema_follows_the_captured_column_order() -> None:
    table = spec(
        column("sale_id", "int"),
        column("amount", "decimal", 10, 2),
        column("note", "varchar"),
    )

    assert row_schema(table) == pa.schema(
        [
            pa.field("sale_id", pa.int32()),
            pa.field("amount", pa.decimal128(10, 2)),
            pa.field("note", pa.large_string()),
        ]
    )


def test_event_schema_puts_the_metadata_columns_ahead_of_the_table() -> None:
    table = spec(column("sale_id", "int"))

    assert event_schema(table) == pa.schema(
        [
            pa.field("start_lsn", pa.large_binary()),
            pa.field("seqval", pa.large_binary()),
            pa.field("operation", pa.int32()),
            pa.field("update_mask", pa.large_binary()),
            pa.field("commit_timestamp", pa.timestamp("us")),
            pa.field("sale_id", pa.int32()),
        ]
    )


def test_metadata_columns_names_the_event_schema_prefix() -> None:
    """inspect() checks business column names against this, so it has to stay in step."""
    assert METADATA_COLUMNS == event_schema(spec(column("sale_id"))).names[:-1]


def test_conform_casts_a_table_the_driver_typed_differently() -> None:
    schema = pa.schema([pa.field("amount", pa.decimal128(10, 2))])
    table = pa.table({"amount": pa.array([Decimal("1.5")], pa.decimal128(18, 4))})

    assert conform(table, schema, "dbo.sales").schema == schema


def test_conform_reorders_to_the_declared_order() -> None:
    schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.int32())])
    table = pa.table({"b": pa.array([1], pa.int32()), "a": pa.array([2], pa.int32())})

    assert conform(table, schema, "dbo.sales").column_names == ["a", "b"]


def test_conform_keeps_the_declared_schema_on_an_empty_table() -> None:
    schema = pa.schema([pa.field("amount", pa.decimal128(10, 2))])

    assert conform(schema.empty_table(), schema, "dbo.sales").num_rows == 0


@pytest.mark.parametrize(
    ("columns", "expected_message"),
    [
        pytest.param({"a": [1], "b": [2]}, "unexpected column 'b'", id="extra"),
        pytest.param({}, "missing column 'a'", id="missing"),
    ],
)
def test_conform_rejects_a_column_set_that_does_not_match(
    columns: dict[str, list[int]], expected_message: str
) -> None:
    schema = pa.schema([pa.field("a", pa.int32())])

    with pytest.raises(ValueError, match=rf"dbo\.sales.*{expected_message}"):
        conform(pa.table(columns), schema, "dbo.sales")


def test_validate_accepts_a_spec_whose_lists_agree() -> None:
    source = [column("sale_id", "int"), column("note", "varchar")]

    assert validate(spec(*source, captured=source)) is None


def test_validate_rejects_a_captured_column_the_source_does_not_have() -> None:
    table = spec(column("sale_id", "int"), captured=[column("dropped_at", "datetime2")])

    with pytest.raises(ValueError, match=r"dropped_at.*no longer in dbo\.sales"):
        validate(table)


def test_validate_rejects_a_captured_column_whose_type_has_drifted() -> None:
    """The change table keeps the type it recorded, so the two reads disagree."""
    table = spec(
        column("sale_id", "int"),
        column("amount", "decimal", 18, 4),
        captured=[column("sale_id", "int"), column("amount", "decimal", 10, 2)],
    )

    with pytest.raises(ValueError, match=r"amount.*decimal\(10,2\).*decimal\(18,4\)"):
        validate(table)


def test_validate_compares_only_the_type_not_the_whole_column() -> None:
    """A column is computed in the source and plain in the change table; not drift."""
    source = column("total_amount", "decimal", 12, 2)
    table = spec(
        column("sale_id", "int"),
        source.model_copy(update={"computed_definition": "([a]*[b])"}),
        captured=[column("sale_id", "int"), source],
    )

    assert validate(table) is None


def test_validate_rejects_a_computed_primary_key() -> None:
    """A key read back as null cannot page the table or match an event."""
    key = column("sale_id", "int").model_copy(
        update={"computed_definition": "([a]*[b])"}
    )

    with pytest.raises(ValueError, match=r"primary key column 'sale_id'.*computed"):
        validate(spec(key, captured=[key]))


def test_validate_rejects_a_primary_key_the_log_does_not_carry() -> None:
    """The merge matches chunk rows against window events on the whole key."""
    table = spec(
        column("sale_id", "int"),
        column("amount", "decimal", 10, 2),
        captured=[column("amount", "decimal", 10, 2)],
    )

    with pytest.raises(ValueError, match=r"primary key column 'sale_id'"):
        validate(table)


@pytest.mark.parametrize("name", METADATA_COLUMNS)
def test_validate_rejects_a_business_column_named_like_a_metadata_column(
    name: str,
) -> None:
    """Two columns of that name would reach polars, which rejects the frame opaquely."""
    source = [column("sale_id", "int"), column(name, "varchar")]

    with pytest.raises(ValueError, match=rf"{name!r}.*change log's own"):
        validate(spec(*source, captured=source))


def test_validate_rejects_a_type_it_cannot_map() -> None:
    source = [column("sale_id", "int"), column("shape", "geography")]

    with pytest.raises(ValueError, match=r"'shape'.*'geography'"):
        validate(spec(*source, captured=source))


def test_conform_reports_the_column_it_could_not_cast() -> None:
    """pyarrow's own message names neither the table nor the column."""
    schema = pa.schema([pa.field("sale_id", pa.int32())])
    table = pa.table({"sale_id": pa.array([datetime(2026, 1, 1)], pa.timestamp("us"))})

    with pytest.raises(ValueError, match=r"dbo\.sales.*sale_id"):
        conform(table, schema, "dbo.sales")
