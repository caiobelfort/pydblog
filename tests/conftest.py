"""
Shared fixtures for the integration tests.

The tests bring up their own SQL Server through testcontainers and run the same
``sql/mssql/01_setup.sql`` the dev-compose uses — so they depend on no
pre-existing database, and on none of the state one accumulates. Docker is the
only requirement.

The container is created once per session: starting SQL Server and enabling CDC
is far too slow to repeat per test.
"""

import os
import time
from pathlib import Path

import logfire
import pytest
from testcontainers.core.container import DockerContainer

from pydblog.connectors import build_connector
from pydblog.connectors.types import TableSpec

# No telemetry during the tests.
logfire.configure(send_to_logfire=False)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETUP_SQL = "/sql/mssql/01_setup.sql"

IMAGE = os.environ.get("PYDBLOG_TEST_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest")
SA_PASSWORD = "Af1uente!LabPwd"
DATABASE = "dblog_lab"

LAB_SCHEMA = "dbo"
LAB_TABLE = "sales"

# The pagination fixture table: a composite primary key and fixed content, so the
# chunked-read assertions are exact and owe nothing to dbo.sales churn. Created by
# section 5 of 01_setup.sql; these constants mirror the rows it seeds.
PAGING_SCHEMA = "dbo"
PAGING_TABLE = "pydblog_paging"
PAGING_ROWS = 10
PAGING_KEYS: list[tuple[int, int]] = [(1, i) for i in range(1, 6)] + [(2, i) for i in range(1, 6)]

# SQL Server is slow to accept connections, and the setup still has to create the
# database and enable CDC on top of that.
STARTUP_TIMEOUT_SECONDS = 300.0

# Maximum wait for the CDC capture job (the SQL Agent scans the log periodically,
# so a change does not land in the change table immediately).
CDC_TIMEOUT_SECONDS = 90.0
POLL_INTERVAL_SECONDS = 1.0

SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


# ---------------------------------------------------------------------------
# SQL helpers
#
# The Protocol deliberately exposes no arbitrary-SQL escape hatch — it is an
# extraction interface. The helpers below reach into `_conn` only to arrange the
# test scenario (DML and waiting on CDC), never to verify a result.
# ---------------------------------------------------------------------------
def execute(connector, sql: str, params: list | None = None) -> None:
    cur = connector._conn.cursor()
    cur.execute(sql, params or [])
    cur.close()


def scalar(connector, sql: str, params: list | None = None):
    cur = connector._conn.cursor()
    cur.execute(sql, params or [])
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def latest_change_lsn(connector, capture_instance: str) -> bytes | None:
    """The highest ``__$start_lsn`` recorded in the change table so far."""
    return scalar(connector, f"SELECT MAX(__$start_lsn) FROM cdc.{capture_instance}_CT")


def wait_for_cdc(connector, capture_instance: str, after_lsn: bytes) -> bytes:
    """
    Block until the capture job records some change after ``after_lsn``.

    Returns the highest ``__$start_lsn`` present in the change table. Comparing
    bytes works because an LSN is a big-endian binary(10).
    """
    deadline = time.monotonic() + CDC_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        latest = latest_change_lsn(connector, capture_instance)
        if latest is not None and latest > after_lsn:
            return latest
        time.sleep(POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"CDC captured no change in {capture_instance} after "
        f"{after_lsn.hex()} within {CDC_TIMEOUT_SECONDS}s. Is the SQL Server "
        f"Agent running? (MSSQL_AGENT_ENABLED=True)"
    )


def wait_for_cdc_ready(connector) -> None:
    """
    Block until ``fn_cdc_get_max_lsn()`` returns a value at all.

    On a freshly started container CDC is enabled but the capture job has not run yet,
    so ``cdc.lsn_time_mapping`` is empty and the database-wide max LSN is NULL. Every
    LSN-based method depends on it, so the session waits here once rather than having
    each test discover it. Goes through raw SQL because ``get_max_lsn()`` itself raises
    on the NULL this is waiting to clear.
    """
    deadline = time.monotonic() + CDC_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if scalar(connector, "SELECT sys.fn_cdc_get_max_lsn()") is not None:
            return
        time.sleep(POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"CDC produced no maximum LSN within {CDC_TIMEOUT_SECONDS}s. Is the SQL Server "
        f"Agent running? (MSSQL_AGENT_ENABLED=True)"
    )


def wait_for_max_lsn(connector, after_lsn: bytes) -> bytes:
    """
    Block until ``get_max_lsn()`` itself moves past ``after_lsn``, and return it.

    Distinct from ``wait_for_cdc``, which polls the change table. The two watermarks
    advance independently, and the snapshot closes its windows at ``get_max_lsn()`` —
    so waiting on the change table and assuming this one caught up is a race.

    Only call this after committing something. The max LSN advances when a new commit
    gets captured and at no other time — it does *not* creep forward on its own as the
    capture job scans, so waiting for it to pass a point nothing moved beyond just burns
    the timeout.
    """
    deadline = time.monotonic() + CDC_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        current = connector.get_max_lsn()
        if current > after_lsn:
            return current
        time.sleep(POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"the max LSN never moved past {after_lsn.hex()} within {CDC_TIMEOUT_SECONDS}s. "
        f"Is the SQL Server Agent running? (MSSQL_AGENT_ENABLED=True)"
    )


def drain_cdc(connector, capture_instance: str) -> bytes:
    """
    Wait until the max LSN covers every change already recorded for the table.

    A snapshot starting after this point has all prior events at or below its opening
    LSN, so they fall in no window and the run sees only its own churn. That is what
    lets a test assert "every emitted row is tagged read".

    The comparison is ``>=``, not ``>``: covering the last change means reaching it. Once
    the capture job has processed it, ``max_lsn == last_change`` exactly, and it will not
    move again until something new commits — so waiting for strictly-greater would hang.
    """
    deadline = time.monotonic() + CDC_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        last_change = latest_change_lsn(connector, capture_instance)
        current = connector.get_max_lsn()
        if last_change is None or current >= last_change:
            return current
        time.sleep(POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"the max LSN never caught up with {capture_instance}'s change table within "
        f"{CDC_TIMEOUT_SECONDS}s. Is the SQL Server Agent running? "
        f"(MSSQL_AGENT_ENABLED=True)"
    )


# ---------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------


def run_sqlcmd(container: DockerContainer, *args: str):
    """
    Run sqlcmd inside the container.

    ``-C`` trusts the self-signed certificate, ``-N`` turns encryption on, ``-I``
    turns QUOTED_IDENTIFIER on — mandatory for DDL/DML against a table with a
    persisted computed column, which dbo.sales has. ``-b`` is what makes a T-SQL
    failure a non-zero exit code; without it sqlcmd exits 0 and a broken setup
    slips straight past the exit_code assertion below.
    """
    return container.exec(
        [SQLCMD, "-S", "localhost", "-U", "sa", "-P", SA_PASSWORD, "-C", "-N", "-I", "-b", *args]
    )


def wait_until_accepting_connections(container: DockerContainer) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    last_output = b""
    while time.monotonic() < deadline:
        result = run_sqlcmd(container, "-Q", "SELECT 1")
        if result.exit_code == 0:
            return
        last_output = result.output
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"SQL Server did not accept connections within {STARTUP_TIMEOUT_SECONDS}s: "
        f"{last_output.decode(errors='replace')}"
    )


@pytest.fixture(scope="session")
def sqlserver():
    """An ephemeral SQL Server, with CDC enabled by the project's setup script."""
    container = (
        DockerContainer(IMAGE)
        .with_env("ACCEPT_EULA", "Y")
        .with_env("MSSQL_SA_PASSWORD", SA_PASSWORD)
        .with_env("MSSQL_PID", "Developer")
        # The SQL Agent runs the CDC capture/cleanup jobs. Without it
        # sp_cdc_enable_table succeeds but the change table is never populated.
        .with_env("MSSQL_AGENT_ENABLED", "True")
        .with_exposed_ports(1433)
        .with_volume_mapping(str(PROJECT_ROOT / "sql"), "/sql", "ro")
    )

    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker unavailable for the integration tests: {exc}")

    try:
        wait_until_accepting_connections(container)

        setup = run_sqlcmd(container, "-i", SETUP_SQL)
        assert setup.exit_code == 0, (
            f"the setup failed: {setup.output.decode(errors='replace')}"
        )

        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def connector(sqlserver):
    """A connector wired to the container."""
    conn = build_connector(
        source_type="mssql",
        host=sqlserver.get_container_host_ip(),
        port=str(sqlserver.get_exposed_port(1433)),
        user="sa",
        password=SA_PASSWORD,
        database=DATABASE,
    )
    conn.connect()
    wait_for_cdc_ready(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spec(connector):
    """The TableSpec of dbo.sales, the CDC-enabled table."""
    table_spec = connector.inspect(LAB_SCHEMA, LAB_TABLE)
    if table_spec.capture_instance is None:
        pytest.skip(f"CDC is not enabled on {table_spec.qualified_name}")
    return table_spec


@pytest.fixture(scope="session")
def paging_spec(connector) -> TableSpec:
    """The TableSpec of dbo.pydblog_paging — composite primary key, fixed content."""
    return connector.inspect(PAGING_SCHEMA, PAGING_TABLE)


@pytest.fixture(scope="session")
def change_window(connector, spec) -> dict:
    """
    Generate an insert + update + delete against one row, and return the LSN
    window holding exactly those events.

    Isolating by LSN window — rather than truncating the table — keeps every test
    independent of what the others wrote: it only ever sees its own events.
    Session-scoped because waiting on CDC is slow and the scenario is reusable.
    """
    from_lsn = connector.get_max_lsn()

    sale_id = scalar(
        connector,
        "INSERT INTO dbo.sales (product_id, customer_id, quantity, unit_price, status) "
        "OUTPUT INSERTED.sale_id "
        "VALUES (?, ?, ?, ?, ?)",
        [901, 902, 3, 12.50, "PENDING"],
    )
    execute(
        connector,
        "UPDATE dbo.sales SET status = ?, updated_at = SYSUTCDATETIME() WHERE sale_id = ?",
        ["COMPLETED", sale_id],
    )
    execute(connector, "DELETE FROM dbo.sales WHERE sale_id = ?", [sale_id])

    wait_for_cdc(connector, spec.capture_instance, from_lsn)
    to_lsn = connector.get_max_lsn()

    return {"from_lsn": from_lsn, "to_lsn": to_lsn, "sale_id": sale_id}


@pytest.fixture(scope="session")
def quiet_lsn(connector, spec, change_window) -> bytes:
    """
    A valid LSN holding no change to the table, for the empty-window case.

    The retention floor predates every captured change, so ``[min_lsn, min_lsn]`` is
    inside CDC's coverage — the range is legal — yet contains nothing.

    ``get_max_lsn()`` will not do: right after a DML it *is* the LSN of that change, so
    ``[max, max]`` comes back with a row. Nor can we wait for it to move past the last
    change, which is what this fixture used to try: the max LSN only advances when a new
    commit gets captured, so during a read-only test phase it never budges and the wait
    times out.

    Depends on ``change_window`` purely for ordering, so the change table is populated by
    the time anything asks for a deliberately empty window.
    """
    return connector.get_min_lsn(spec.capture_instance)
