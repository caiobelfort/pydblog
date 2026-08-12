"""
MSSQLConnector tests.

Conformance with the ``SourceConnector`` Protocol is not tested here: the type
checker (pyright/mypy) is what verifies that. ``build_connector`` is already
annotated ``-> SourceConnector`` and hands back an ``MSSQLConnector``, so any
signature drift surfaces in static analysis — a runtime test for it would be both
redundant and weaker.

What is left is what only the database can answer: that the generated SQL is valid
T-SQL, that CDC returns the expected events, and that types land correctly in the
DataFrame. Hence most of this is integration, against an ephemeral SQL Server the
tests bring up through testcontainers (see ``conftest.py``).
"""

from datetime import datetime
from decimal import Decimal

import pytest
from polars import Binary, DataFrame, Datetime, Decimal as PlDecimal, Int32, String

from pydblog.connectors import build_connector
from pydblog.connectors.mssql import (
    LSN_WIDTH,
    OP_DELETE,
    OP_INSERT,
    OP_UPDATE_AFTER,
    OP_UPDATE_BEFORE,
    MSSQLConnector,
)
from pydblog.connectors.types import TableSpec

from conftest import (
    DATABASE,
    LAB_SCHEMA,
    LAB_TABLE,
    PAGING_KEYS,
    PAGING_ROWS,
    SA_PASSWORD,
    execute,
)

# Metadata columns read_event_log exposes, in order, ahead of the business ones.
CDC_METADATA_COLUMNS = ["start_lsn", "seqval", "operation", "update_mask"]


def make_connector() -> MSSQLConnector:
    return MSSQLConnector(
        host="localhost", port="1433", user="u", password="p", database="d"
    )


def make_spec(capture_instance: str | None) -> TableSpec:
    return TableSpec(
        source_schema="dbo",
        source_table="sales",
        pk_columns=["sale_id"],
        business_columns=["sale_id"],
        capture_instance=capture_instance,
    )


def make_read_spec(**overrides) -> TableSpec:
    """A valid read_table spec, with one field at a time swapped out by the test."""
    fields = {
        "source_schema": "dbo",
        "source_table": "sales",
        "pk_columns": ["sale_id"],
        "business_columns": ["sale_id", "product_id", "unit_price"],
    }
    return TableSpec(**(fields | overrides))


# ---------------------------------------------------------------------------
# Lifecycle — instantiating does not connect, so no database needed
# ---------------------------------------------------------------------------


def test_starts_disconnected():
    assert make_connector()._conn is None


def test_close_without_connect_is_noop():
    conn = make_connector()
    conn.close()
    assert conn._conn is None


def test_application_name_defaults():
    assert make_connector()._application_name == "Lakehouse DBLog"


def test_kwargs_override_connection_options():
    conn = MSSQLConnector(
        host="localhost",
        port="1433",
        user="u",
        password="p",
        database="d",
        encrypt="no",
        trust_server_certificate="no",
        application_name="custom",
    )
    assert (conn._encrypt, conn._trust_server_certificate, conn._application_name) == (
        "no",
        "no",
        "custom",
    )


# ---------------------------------------------------------------------------
# read_event_log — input validation, also needs no database
# ---------------------------------------------------------------------------


def test_read_event_log_requires_capture_instance():
    with pytest.raises(ValueError, match="capture instance"):
        make_connector().read_event_log(make_spec(None), b"\x00" * 10, b"\x01" * 10)


@pytest.mark.parametrize(
    "capture_instance",
    [
        "dbo_sales; DROP TABLE dbo.sales--",
        "dbo_sales(1)",
        "dbo sales",
        "dbo.sales",
        "",
    ],
)
def test_read_event_log_rejects_unsafe_capture_instance(capture_instance):
    """
    The capture instance goes into the function name (``fn_cdc_get_all_changes_<ci>``),
    so it cannot be bound as a parameter — validation is the only barrier against
    injection.
    """
    with pytest.raises(ValueError, match="Invalid capture instance"):
        make_connector().read_event_log(
            make_spec(capture_instance), b"\x00" * 10, b"\x01" * 10
        )


# ---------------------------------------------------------------------------
# read_table — the generated query
#
# read_table's risk lives in the string, not the I/O: the composite ORDER BY, the
# optional range bounds and the limit=0 branch are purely textual bugs. Testing them
# here, with no container, gives a readable failure and costs nothing in SQL Server
# startup.
# ---------------------------------------------------------------------------


def build_query(
    start_pk: int | None = None, end_pk: int | None = None, limit: int = 0, **overrides
) -> str:
    query, _ = make_connector()._build_read_table_query(
        make_read_spec(**overrides), start_pk, end_pk, limit
    )
    return query


def build_params(
    start_pk: int | None = None, end_pk: int | None = None, limit: int = 0
) -> list[int]:
    _, params = make_connector()._build_read_table_query(
        make_read_spec(), start_pk, end_pk, limit
    )
    return params


def test_build_read_table_query_with_both_bounds():
    assert build_query(start_pk=10, end_pk=20) == (
        "SELECT [sale_id], [product_id], [unit_price] "
        "FROM [dbo].[sales] "
        "WHERE [sale_id] >= ? AND [sale_id] < ? "
        "ORDER BY [sale_id]"
    )


def test_build_read_table_query_binds_the_bounds_in_order():
    assert build_params(start_pk=10, end_pk=20) == [10, 20]


def test_build_read_table_query_with_only_a_lower_bound():
    """The open-ended tail of a plan: everything from a key onwards."""
    assert build_query(start_pk=10) == (
        "SELECT [sale_id], [product_id], [unit_price] "
        "FROM [dbo].[sales] "
        "WHERE [sale_id] >= ? "
        "ORDER BY [sale_id]"
    )
    assert build_params(start_pk=10) == [10]


def test_build_read_table_query_with_only_an_upper_bound():
    assert build_query(end_pk=20) == (
        "SELECT [sale_id], [product_id], [unit_price] "
        "FROM [dbo].[sales] "
        "WHERE [sale_id] < ? "
        "ORDER BY [sale_id]"
    )
    assert build_params(end_pk=20) == [20]


def test_build_read_table_query_without_bounds_omits_the_where_clause():
    """Both bounds default to None, which makes this the first path any caller hits."""
    query = build_query()

    assert query == (
        "SELECT [sale_id], [product_id], [unit_price] "
        "FROM [dbo].[sales] "
        "ORDER BY [sale_id]"
    )
    assert "WHERE" not in query
    assert build_params() == []


def test_build_read_table_query_lower_bound_is_inclusive_and_upper_exclusive():
    """
    Half-open ranges are what let neighbouring chunks abut without overlapping: the
    key at a boundary belongs to exactly one of them.
    """
    query = build_query(start_pk=10, end_pk=20)

    assert "[sale_id] >= ?" in query
    assert "[sale_id] < ?" in query
    assert ">" not in query.replace(">=", "")


def test_build_read_table_query_bounds_only_the_leading_pk_column():
    """
    Against a composite key the range still applies to the leading column alone —
    which partitions the table completely, since every row has exactly one value for
    it — while the ORDER BY keeps the whole key.
    """
    query = build_query(
        start_pk=1,
        end_pk=2,
        pk_columns=["tenant_id", "item_id"],
        business_columns=["tenant_id", "item_id", "label"],
    )

    assert "WHERE [tenant_id] >= ? AND [tenant_id] < ?" in query
    assert "ORDER BY [tenant_id], [item_id]" in query
    assert "[item_id] >=" not in query


def test_build_read_table_query_with_a_limit_cap():
    query = build_query(start_pk=10, end_pk=20, limit=5)

    assert query.endswith("ORDER BY [sale_id] OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY")
    assert build_params(start_pk=10, end_pk=20, limit=5) == [10, 20, 5]


def test_build_read_table_query_without_a_limit_omits_offset_and_fetch():
    """
    limit=0 means uncapped. T-SQL rejects FETCH NEXT 0 ROWS ONLY, and OFFSET is only
    there because FETCH cannot appear without it — so both clauses go together.
    """
    query = build_query(start_pk=10, end_pk=20)

    assert "FETCH" not in query
    assert "OFFSET" not in query


@pytest.mark.parametrize(
    "pk_columns, expected",
    [
        (["tenant_id", "item_id"], "ORDER BY [tenant_id], [item_id]"),
        (["item_id", "tenant_id"], "ORDER BY [item_id], [tenant_id]"),
    ],
)
def test_build_read_table_query_orders_by_full_pk_in_key_order(pk_columns, expected):
    """
    The order comes from the key ordinality, not from sorting alphabetically: the
    reversed pair has to produce the reversed ORDER BY.
    """
    query = build_query(
        limit=10, pk_columns=pk_columns, business_columns=[*pk_columns, "label"]
    )
    assert expected in query


def test_build_read_table_query_selects_business_columns_in_order():
    query = build_query(business_columns=["unit_price", "sale_id", "product_id"])
    assert query.startswith("SELECT [unit_price], [sale_id], [product_id] ")


def test_build_read_table_query_does_not_add_pk_to_selected_columns():
    """
    The PK belongs in the ORDER BY, not the SELECT: adding it on its own would
    silently violate the column set the caller asked for.
    """
    query = build_query(pk_columns=["tenant_id", "item_id"], business_columns=["label"])

    assert query.startswith("SELECT [label] FROM ")
    assert "ORDER BY [tenant_id], [item_id]" in query


def test_read_table_rejects_empty_pk_columns():
    with pytest.raises(ValueError, match="primary key"):
        make_connector().read_table(make_read_spec(pk_columns=[]))


def test_read_table_rejects_empty_business_columns():
    with pytest.raises(ValueError, match="business columns"):
        make_connector().read_table(make_read_spec(business_columns=[]))


@pytest.mark.parametrize("limit", [-1, -100])
def test_read_table_rejects_a_negative_limit(limit):
    with pytest.raises(ValueError, match="must be >= 0"):
        make_connector().read_table(make_read_spec(), limit=limit)


@pytest.mark.parametrize("start_pk, end_pk", [(10, 10), (20, 10), (0, -5)])
def test_read_table_rejects_inverted_bounds(start_pk, end_pk):
    """
    An inverted or empty range is a chunk-planning bug, not a legitimate request for
    zero rows — failing beats silently reading nothing.
    """
    with pytest.raises(ValueError, match="start_pk must be < end_pk"):
        make_connector().read_table(make_read_spec(), start_pk=start_pk, end_pk=end_pk)


@pytest.mark.parametrize(
    "field, value",
    [
        ("source_schema", "dbo; DROP TABLE dbo.sales--"),
        ("source_schema", ""),
        ("source_table", "sales WHERE 1=1"),
        # The two cases carrying ']' are the point of this test: the bracket closes
        # the delimiter and whatever follows becomes SQL. Only validation stops it.
        ("source_table", "sales]; DROP TABLE dbo.sales--"),
        ("pk_columns", ["sale_id DESC"]),
        ("pk_columns", ["sale_id]--"]),
        ("business_columns", ["*"]),
        ("business_columns", ["sale_id], (SELECT TOP 1 name FROM sys.tables) AS [x"]),
    ],
)
def test_read_table_rejects_unsafe_identifiers(field, value):
    with pytest.raises(ValueError, match="Invalid"):
        make_connector().read_table(make_read_spec(**{field: value}))


# ---------------------------------------------------------------------------
# read_pk_range — the generated query
# ---------------------------------------------------------------------------


def test_build_pk_range_query_uses_the_leading_pk_column():
    query = make_connector()._build_pk_range_query(make_read_spec())
    assert query == "SELECT MIN([sale_id]), MAX([sale_id]) FROM [dbo].[sales]"


def test_build_pk_range_query_ignores_trailing_pk_columns():
    """Chunking slices one dimension, so only the leading key column is measured."""
    spec = make_read_spec(
        pk_columns=["tenant_id", "item_id"], business_columns=["tenant_id", "item_id"]
    )
    query = make_connector()._build_pk_range_query(spec)

    assert query == "SELECT MIN([tenant_id]), MAX([tenant_id]) FROM [dbo].[sales]"


def test_read_pk_range_rejects_empty_pk_columns():
    with pytest.raises(ValueError, match="primary key"):
        make_connector().read_pk_range(make_read_spec(pk_columns=[]))


@pytest.mark.parametrize(
    "field, value",
    [
        ("source_schema", "dbo]--"),
        ("source_table", "sales]; DROP TABLE dbo.sales--"),
        ("pk_columns", ["sale_id]--"]),
    ],
)
def test_read_pk_range_rejects_unsafe_identifiers(field, value):
    with pytest.raises(ValueError, match="Invalid"):
        make_connector().read_pk_range(make_read_spec(**{field: value}))


# ---------------------------------------------------------------------------
# Connection and LSN
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_connect_is_idempotent(connector):
    connector.connect()
    connector.connect()
    assert connector.get_max_lsn() is not None


@pytest.mark.integration
def test_reads_connect_lazily(sqlserver):
    """
    Every read method starts by calling connect(), so the caller never has to connect
    first — constructing and reading straight away has to work.
    """
    conn = build_connector(
        source_type="mssql",
        host=sqlserver.get_container_host_ip(),
        port=str(sqlserver.get_exposed_port(1433)),
        user="sa",
        password=SA_PASSWORD,
        database=DATABASE,
    )
    try:
        assert conn._conn is None
        assert conn.inspect(LAB_SCHEMA, LAB_TABLE).pk_columns == ["sale_id"]
    finally:
        conn.close()


@pytest.mark.integration
def test_get_max_lsn_returns_an_lsn(connector):
    max_lsn = connector.get_max_lsn()
    assert isinstance(max_lsn, bytes)
    assert len(max_lsn) == 10


@pytest.mark.integration
def test_map_lsn_to_timestamp_returns_datetime(connector):
    assert isinstance(connector.map_lsn_to_timestamp(connector.get_max_lsn()), datetime)


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_inspect_finds_pk_and_capture_instance(spec):
    assert spec.pk_columns == ["sale_id"]
    assert spec.capture_instance == "dbo_sales"
    assert spec.qualified_name == f"{LAB_SCHEMA}.{LAB_TABLE}"


@pytest.mark.integration
def test_inspect_pk_is_subset_of_business_columns(spec):
    assert set(spec.pk_columns) <= set(spec.business_columns)


@pytest.mark.integration
def test_inspect_excludes_computed_columns(spec):
    """
    total_amount is a persisted computed column. CDC does not capture it, so inspect()
    has to leave it out — otherwise read_event_log would build a SELECT naming a column
    that does not exist in the change table.
    """
    assert "total_amount" not in spec.business_columns
    assert "unit_price" in spec.business_columns


@pytest.mark.integration
def test_inspect_raises_without_primary_key(connector):
    execute(
        connector,
        "IF OBJECT_ID('dbo.pydblog_no_pk') IS NULL "
        "CREATE TABLE dbo.pydblog_no_pk (id INT NOT NULL)",
    )
    try:
        with pytest.raises(ValueError, match="does not have primary key"):
            connector.inspect("dbo", "pydblog_no_pk")
    finally:
        execute(connector, "DROP TABLE IF EXISTS dbo.pydblog_no_pk")


@pytest.mark.integration
def test_inspect_unknown_table_raises(connector):
    with pytest.raises(ValueError):
        connector.inspect("dbo", "table_that_does_not_exist")


# ---------------------------------------------------------------------------
# read_event_log — integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_read_event_log_returns_dataframe(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    assert isinstance(events, DataFrame)


@pytest.mark.integration
def test_read_event_log_column_order_is_metadata_then_business(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    assert events.columns == CDC_METADATA_COLUMNS + spec.business_columns


@pytest.mark.integration
def test_read_event_log_is_ordered_by_lsn(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    assert events.equals(events.sort(["start_lsn", "seqval"]))


@pytest.mark.integration
def test_read_event_log_captures_insert_update_and_delete(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    own = events.filter(events["sale_id"] == change_window["sale_id"])
    assert own["operation"].to_list() == [OP_INSERT, OP_UPDATE_AFTER, OP_DELETE]


@pytest.mark.integration
def test_read_event_log_update_carries_only_after_image(connector, spec, change_window):
    """
    With row_filter 'all' the update returns only the after image (operation 4); the
    before image (operation 3) never shows up.
    """
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    own = events.filter(events["sale_id"] == change_window["sale_id"])

    assert OP_UPDATE_BEFORE not in own["operation"].to_list()
    assert own.filter(own["operation"] == OP_UPDATE_AFTER)["status"].to_list() == ["COMPLETED"]
    assert own.filter(own["operation"] == OP_INSERT)["status"].to_list() == ["PENDING"]


@pytest.mark.integration
def test_read_event_log_delete_carries_last_known_values(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    own = events.filter(events["sale_id"] == change_window["sale_id"])
    deleted = own.filter(own["operation"] == OP_DELETE)

    assert deleted.height == 1
    assert deleted["status"].to_list() == ["COMPLETED"]


@pytest.mark.integration
def test_read_event_log_maps_sql_types_via_arrow(connector, spec, change_window):
    """
    The fetch goes through ``cur.arrow()``, so types come from the result set metadata
    instead of being inferred from Python objects. The case that matters is unit_price:
    DECIMAL(10,2) has to stay exact, not turn into a float.
    """
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )

    assert events.schema["unit_price"] == PlDecimal(precision=10, scale=2)
    assert events.schema["start_lsn"] == Binary
    assert events.schema["operation"] == Int32
    assert events.schema["status"] == String
    assert isinstance(events.schema["created_at"], Datetime)


@pytest.mark.integration
def test_read_event_log_preserves_decimal_precision(connector, spec, change_window):
    events = connector.read_event_log(
        spec, change_window["from_lsn"], change_window["to_lsn"]
    )
    own = events.filter(events["sale_id"] == change_window["sale_id"])
    assert own["unit_price"].to_list() == [Decimal("12.50")] * 3


@pytest.mark.integration
def test_read_event_log_returns_none_when_range_has_no_events(connector, spec, quiet_lsn):
    """With no event at all in the range, the return is None rather than an empty DataFrame."""
    assert connector.read_event_log(spec, quiet_lsn, quiet_lsn) is None


# ---------------------------------------------------------------------------
# read_table — integration
#
# Against dbo.pydblog_paging (the paging_spec fixture), which has a composite PK and
# fixed content: the range assertions are exact and owe nothing to dbo.sales churn.
# Its leading key column, tenant_id, holds 1 and 2 — five rows each.
# ---------------------------------------------------------------------------

TENANT_1_KEYS = PAGING_KEYS[:5]
TENANT_2_KEYS = PAGING_KEYS[5:]


def keys_of(df: DataFrame, spec: TableSpec) -> list[tuple]:
    """The result's PK columns, as a list of tuples, in the order they were read."""
    return df.select(spec.pk_columns).rows()


@pytest.mark.integration
def test_read_table_returns_dataframe(connector, paging_spec):
    df = connector.read_table(paging_spec)
    assert isinstance(df, DataFrame)
    assert df.height == PAGING_ROWS


@pytest.mark.integration
def test_read_table_column_order_matches_business_columns(connector, paging_spec):
    assert connector.read_table(paging_spec).columns == paging_spec.business_columns


@pytest.mark.integration
def test_read_table_unbounded_reads_the_whole_table_in_key_order(connector, paging_spec):
    assert keys_of(connector.read_table(paging_spec), paging_spec) == PAGING_KEYS


@pytest.mark.integration
def test_read_table_lower_bound_is_inclusive(connector, paging_spec):
    df = connector.read_table(paging_spec, start_pk=2)
    assert keys_of(df, paging_spec) == TENANT_2_KEYS


@pytest.mark.integration
def test_read_table_upper_bound_is_exclusive(connector, paging_spec):
    """end_pk=2 must stop before tenant 2, not include it."""
    df = connector.read_table(paging_spec, end_pk=2)
    assert keys_of(df, paging_spec) == TENANT_1_KEYS


@pytest.mark.integration
def test_read_table_bounded_range_selects_one_leading_key(connector, paging_spec):
    df = connector.read_table(paging_spec, start_pk=1, end_pk=2)
    assert keys_of(df, paging_spec) == TENANT_1_KEYS


@pytest.mark.integration
def test_read_table_orders_by_the_full_pk_across_a_leading_key_boundary(connector, paging_spec):
    """
    A cap of 6 takes all of tenant 1 and the first row of tenant 2. An ORDER BY on
    tenant_id alone could not produce that slice reliably — it is the trailing key
    column that decides which row of tenant 2 comes first.
    """
    df = connector.read_table(paging_spec, limit=6)
    assert keys_of(df, paging_spec) == [*TENANT_1_KEYS, (2, 1)]


@pytest.mark.integration
def test_read_table_limit_caps_rows_within_a_range(connector, paging_spec):
    df = connector.read_table(paging_spec, start_pk=1, end_pk=2, limit=3)
    assert keys_of(df, paging_spec) == TENANT_1_KEYS[:3]


@pytest.mark.integration
def test_read_table_limit_larger_than_the_range_returns_all_of_it(connector, paging_spec):
    assert connector.read_table(paging_spec, limit=1000).height == PAGING_ROWS


@pytest.mark.integration
def test_read_table_range_past_the_end_returns_an_empty_dataframe(connector, paging_spec):
    """
    Unlike read_event_log, which returns None for an empty window, an empty key range
    is a legitimate answer here — chunk_size is a key width, so sparse and empty
    ranges are expected. The DataFrame comes back with no rows but the right columns,
    because arrow carries the result set schema even at zero rows.
    """
    df = connector.read_table(paging_spec, start_pk=100, end_pk=200)

    assert df.height == 0
    assert df.columns == paging_spec.business_columns


def plan_chunks(min_pk: int, max_pk: int, chunk_size: int) -> list[tuple[int, int]]:
    """
    Half-open key ranges covering [min_pk, max_pk].

    A test-local planner on purpose: slicing a key space is the caller's concern, not the
    source's, so the connector does not offer it. The two tests below still need one to
    prove that read_table's ranges compose into exact coverage.
    """
    return [
        (start, min(start + chunk_size, max_pk + 1))
        for start in range(min_pk, max_pk + 1, chunk_size)
    ]


@pytest.mark.integration
def test_read_table_chunks_cover_every_row_exactly_once(connector, paging_spec):
    """
    The chunk loop over the real key bounds: every row read once, nothing repeated,
    nothing missed.
    """
    bounds = connector.read_pk_range(paging_spec)
    assert bounds == (1, 2)

    keys: list[tuple] = []
    for start_pk, end_pk in plan_chunks(*bounds, chunk_size=1):
        keys += keys_of(
            connector.read_table(paging_spec, start_pk=start_pk, end_pk=end_pk),
            paging_spec,
        )

    assert keys == PAGING_KEYS
    assert len(set(keys)) == len(keys)


@pytest.mark.integration
def test_read_table_chunks_cover_dbo_sales_exactly_once(connector, spec):
    """
    The same loop against a single-column IDENTITY key, with a chunk size well below
    the row count so several chunks are exercised.
    """
    bounds = connector.read_pk_range(spec)
    expected = connector.read_table(spec)["sale_id"].to_list()

    read: list[int] = []
    for start_pk, end_pk in plan_chunks(*bounds, chunk_size=2):
        read += connector.read_table(
            spec, start_pk=start_pk, end_pk=end_pk
        )["sale_id"].to_list()

    assert read == expected
    assert len(set(read)) == len(read)


@pytest.mark.integration
def test_read_table_maps_sql_types_via_arrow(connector, paging_spec):
    schema = connector.read_table(paging_spec, limit=1).schema

    assert schema["amount"] == PlDecimal(precision=10, scale=2)
    assert schema["tenant_id"] == Int32
    assert schema["label"] == String


# ---------------------------------------------------------------------------
# read_pk_range and get_min_lsn — integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_read_pk_range_returns_the_leading_key_bounds(connector, paging_spec):
    assert connector.read_pk_range(paging_spec) == (1, 2)


@pytest.mark.integration
def test_read_pk_range_matches_the_table_contents(connector, spec):
    sale_ids = connector.read_table(spec)["sale_id"].to_list()
    assert connector.read_pk_range(spec) == (min(sale_ids), max(sale_ids))


@pytest.mark.integration
def test_read_pk_range_returns_none_for_an_empty_table(connector):
    execute(
        connector,
        "IF OBJECT_ID('dbo.pydblog_empty') IS NULL "
        "CREATE TABLE dbo.pydblog_empty (id INT NOT NULL PRIMARY KEY)",
    )
    try:
        empty_spec = connector.inspect("dbo", "pydblog_empty")
        assert connector.read_pk_range(empty_spec) is None
    finally:
        execute(connector, "DROP TABLE IF EXISTS dbo.pydblog_empty")


@pytest.mark.integration
def test_read_pk_range_rejects_a_non_integer_leading_key(connector):
    """Value arithmetic cannot slice a key space it cannot count."""
    execute(
        connector,
        "IF OBJECT_ID('dbo.pydblog_strkey') IS NULL "
        "CREATE TABLE dbo.pydblog_strkey (code NVARCHAR(10) NOT NULL PRIMARY KEY)",
    )
    try:
        execute(connector, "INSERT INTO dbo.pydblog_strkey (code) VALUES (N'a'), (N'b')")
        str_spec = connector.inspect("dbo", "pydblog_strkey")

        with pytest.raises(ValueError, match="non-integer leading primary key"):
            connector.read_pk_range(str_spec)
    finally:
        execute(connector, "DROP TABLE IF EXISTS dbo.pydblog_strkey")


@pytest.mark.integration
def test_get_min_lsn_returns_an_lsn(connector, spec):
    """
    Regression: the capture instance used to be interpolated into a quoted '?', so it
    was never bound and CDC was asked about a capture instance literally named "?".
    """
    min_lsn = connector.get_min_lsn(spec.capture_instance)

    assert isinstance(min_lsn, bytes)
    assert len(min_lsn) == 10
    assert min_lsn <= connector.get_max_lsn()


@pytest.mark.integration
def test_get_min_lsn_rejects_an_unknown_capture_instance(connector):
    with pytest.raises(ValueError, match="no minimum LSN"):
        connector.get_min_lsn("dbo_not_a_capture_instance")


@pytest.mark.integration
def test_read_table_orders_by_pk_not_present_in_selected_columns(connector, paging_spec):
    """
    The PK stays out of the SELECT but remains in the ORDER BY — T-SQL allows it, and
    it is what lets us read only the requested columns without losing the chunk order.
    """
    spec_without_pk = paging_spec.model_copy(update={"business_columns": ["label"]})
    df = connector.read_table(spec_without_pk)

    assert df.columns == ["label"]
    assert df["label"].to_list() == [f"t{tenant}i{item}" for tenant, item in PAGING_KEYS]


@pytest.mark.integration
def test_read_table_reads_the_inspected_spec_of_dbo_sales(connector, spec):
    """
    The inspect -> read_table path against the lab table. No count assertion: what is
    in dbo.sales depends on which scenario fixtures have already run.
    """
    df = connector.read_table(spec)

    assert df.columns == spec.business_columns
    assert "total_amount" not in df.columns  # computed, excluded by inspect
    assert df["sale_id"].to_list() == sorted(df["sale_id"].to_list())
    assert df.schema["unit_price"] == PlDecimal(precision=10, scale=2)


# ---------------------------------------------------------------------------
# increment_lsn — pure arithmetic, no database
# ---------------------------------------------------------------------------


def lsn_of(value: int) -> bytes:
    return value.to_bytes(LSN_WIDTH, "big")


@pytest.mark.parametrize("value", [0, 1, 255, 256, 2**40, 2**79])
def test_increment_lsn_returns_the_next_value(value):
    assert make_connector().increment_lsn(lsn_of(value)) == lsn_of(value + 1)


def test_increment_lsn_carries_across_a_byte_boundary():
    """0x00..FF -> 0x01..00. The carry is why this is not a single-byte bump."""
    assert make_connector().increment_lsn(bytes([0] * 9 + [0xFF])) == bytes([0] * 8 + [1, 0])


def test_increment_lsn_result_sorts_after_the_input():
    """
    Byte order is numeric order for a big-endian fixed-width LSN, and the whole project
    compares LSNs as raw bytes. That has to survive the carry.
    """
    before = bytes([0] * 9 + [0xFF])
    assert make_connector().increment_lsn(before) > before


@pytest.mark.parametrize("width", [0, 1, 9, 11, 20])
def test_increment_lsn_rejects_the_wrong_width(width):
    with pytest.raises(ValueError, match="must be 10 bytes"):
        make_connector().increment_lsn(bytes(width))


def test_increment_lsn_rejects_the_maximum_lsn():
    """Wrapping to zero would silently rewind the read position and re-read the log."""
    with pytest.raises(ValueError, match="maximum LSN"):
        make_connector().increment_lsn(bytes([0xFF] * LSN_WIDTH))
