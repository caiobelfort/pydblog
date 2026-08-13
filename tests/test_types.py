"""
Tests for the source metadata models.

``TableSpec`` is what ``inspect()`` returns and every read is driven from, so what
matters here is that the two column lists stay distinct and that the name list the
query builders use is derived from the captured one rather than kept alongside it.
"""

import pytest
from pydantic import ValidationError

from pydblog.connectors.types import ColumnSpec, TableSpec


def column(name: str, type_name: str = "int", precision: int = 10, scale: int = 0) -> ColumnSpec:
    return ColumnSpec(name=name, type_name=type_name, precision=precision, scale=scale)


def computed_column(
    name: str, type_name: str = "int", precision: int = 10, scale: int = 0
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        type_name=type_name,
        precision=precision,
        scale=scale,
        computed_definition="([quantity]*[unit_price])",
    )


def test_column_spec_carries_the_type_metadata() -> None:
    spec = column("unit_price", "decimal", precision=10, scale=2)

    assert (spec.name, spec.type_name, spec.precision, spec.scale) == (
        "unit_price",
        "decimal",
        10,
        2,
    )


def test_signature_is_the_type_alone() -> None:
    assert column("unit_price", "decimal", 10, 2).signature == ("decimal", 10, 2)


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        pytest.param(column("amount", "decimal", 10, 2), True, id="identical"),
        pytest.param(column("renamed", "decimal", 10, 2), False, id="different-name"),
        pytest.param(column("amount", "DECIMAL", 10, 2), True, id="different-case"),
        pytest.param(column("amount", "decimal", 18, 2), False, id="wider-precision"),
        pytest.param(column("amount", "decimal", 10, 4), False, id="deeper-scale"),
        pytest.param(column("amount", "numeric", 10, 2), False, id="other-type"),
    ],
)
def test_equality_compares_everything_but_whether_a_column_is_computed(
    other: ColumnSpec, expected: bool
) -> None:
    assert (column("amount", "decimal", 10, 2) == other) is expected


def test_equality_ignores_whether_a_column_is_computed() -> None:
    """A column is computed in the source and a plain column in the change table."""
    source = computed_column("total_amount", "decimal", 12, 2)

    assert source == column("total_amount", "decimal", 12, 2)


def test_a_column_is_computed_when_it_carries_a_formula() -> None:
    """The formula is the whole of it, so the two can never disagree."""
    assert computed_column("total_amount", "decimal", 12, 2).is_computed
    assert not column("amount", "decimal", 12, 2).is_computed


def test_the_formula_is_kept_for_a_consumer_to_recompute_from() -> None:
    """CDC never fills a computed column, so downstream has to derive it itself."""
    spec = computed_column("total_amount", "decimal", 12, 2)

    assert spec.computed_definition == "([quantity]*[unit_price])"


def test_a_column_is_never_equal_to_something_else() -> None:
    assert column("amount", "decimal", 10, 2) != ("decimal", 10, 2)


def test_equal_columns_hash_alike() -> None:
    """Equality without a matching hash would break any set or dict of columns."""
    assert len({column("amount", "int"), computed_column("amount", "int")}) == 1


def test_business_columns_derives_from_the_captured_columns_in_order() -> None:
    spec = TableSpec(
        source_schema="dbo",
        source_table="sales",
        pk_columns=["sale_id"],
        columns=[column("sale_id"), column("amount"), column("internal_note", "varchar")],
        captured_columns=[column("sale_id"), column("amount")],
    )

    assert spec.business_columns == ["sale_id", "amount"]


def test_qualified_name_joins_schema_and_table() -> None:
    spec = TableSpec(
        source_schema="dbo",
        source_table="sales",
        pk_columns=["sale_id"],
        columns=[column("sale_id")],
        captured_columns=[column("sale_id")],
    )

    assert spec.qualified_name == "dbo.sales"


def test_business_columns_is_derived_not_stored() -> None:
    """A stored copy could drift from the list it is supposed to name."""
    assert "business_columns" not in TableSpec.model_fields


def test_spec_is_frozen() -> None:
    """inspect() builds it complete, and every read is driven from it unchanged."""
    spec = TableSpec(
        source_schema="dbo",
        source_table="sales",
        pk_columns=["sale_id"],
        columns=[column("sale_id")],
        captured_columns=[column("sale_id")],
    )

    with pytest.raises(ValidationError):
        spec.capture_instance = "dbo_sales"
