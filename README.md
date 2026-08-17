# pydblog

Streams a SQL Server table's changes, optionally interleaved with a chunked dump of
its rows, using the [DBLog](https://arxiv.org/abs/2010.12597) algorithm over CDC.

One call reads one batch and returns it as a polars DataFrame, or None once there is
nothing left to read. Every frame shares one schema — change events and dump rows
alike — so no frame needs reconciling against another. You write each one and commit,
and the commit is what a later run resumes from:

```python
from pydblog.dblog import DBLog

with DBLog(
    source_type="mssql",
    host="localhost", port="1433",
    user="sa", password="...", database="dblog_lab",
    schema="dbo", table="sales", dump="sales-backfill",
    chunk_size=1000,
) as dblog:
    while (frame := dblog.fetch()) is not None:
        frame.write_delta("s3://lake/sales", mode="append")
        dblog.commit()  # only now is that batch's position recorded
```

An instance is one run. The table and the dump identify the run rather than any batch
of it, so they are constructor arguments and `fetch()` takes none: reading a second
table, or the same table under a second dump name, is a second `DBLog`.

Nothing is buffered and nothing is lazy: the batch is read by the call, and its size
is bounded by `chunk_size` however large the table is. The first call inspects the
table and loads whatever progress is recorded; the rest just advance.

That loop runs until the table is walked *and* the log is caught up. To stop at the
end of the table instead — a backfill rather than a tail — loop on `dump_done`:

```python
while not dblog.dump_done:
    frame = dblog.fetch()
    if frame is not None:
        frame.write_delta("s3://lake/sales", mode="append")
        dblog.commit()
```

`dump_done` is about the table walk, so it is `True` from the start when there is no
`dump` — that loop does nothing at all for a log-only run, which is the honest answer
to "walk the table" when there is no table walk to do. Log-only callers want the
`is not None` shape above, or a poll of their own.

Rows that came off the table rather than out of the log are marked with an all-zero
`start_lsn` and `operation = 0`, a position CDC never issues.

Naming a `dump` makes the run walk the table as well as the log, and gives `commit()`
a name to record progress under, so an interrupted run resumes at the last chunk you
committed. Leaving it out reads the change log only, and then requires a `from_lsn`.

`commit()` is yours to call because only you know when the data is safe. A frame you
received but never committed is read again on the next run; a position recorded before
your write succeeded would put those rows out of reach for good.

Worth one `commit()` after the loop as well as inside it. A short chunk ends the dump
and arrives as a frame, so committing that frame records the dump as finished — but
when the row count is an exact multiple of `chunk_size` there is no short chunk, and
the end is found by a batch that comes back empty with nothing to commit alongside.
Skipping that last call costs the next run one empty read to rediscover the end, and
nothing else.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker, for the integration tests only

## Setup

```bash
uv sync
```

## Running the tests

The fast suite needs nothing but Python:

```bash
uv run pytest -m "not integration"
```

The integration suite starts a real SQL Server through testcontainers, enables CDC on
it and runs `sql/mssql/01_setup.sql`. It needs Docker:

```bash
uv run pytest
```

**Stop the dev-compose server first if it is running.** `dev-compose.yml` starts a SQL
Server on port 1433 with no memory limit, and testcontainers will start a second one
alongside it. Two instances is enough to get both OOM-killed, and the failure — an
exit code 137 and containers exited 255 — looks nothing like its cause:

```bash
docker compose -f dev-compose.yml down
```

## Type checking

`pyrefly` is a global tool rather than a project dependency:

```bash
uv tool install pyrefly
uv run pyrefly check src
```

## A local lab server

For poking at CDC by hand, `dev-compose.yml` brings up a SQL Server and runs the same
setup script the tests use:

```bash
docker compose -f dev-compose.yml up -d
```

It creates `dblog_lab` with `dbo.sales` (CDC enabled, and a persisted computed column
that CDC leaves NULL — see the ADR), plus fixtures for keyset pagination and schema
drift.

## Layout

| Path | What lives there |
| --- | --- |
| `src/pydblog/dblog.py` | The algorithm: chunk, window, supersede, commit |
| `src/pydblog/connectors/base.py` | The `SourceConnector` Protocol every source implements |
| `src/pydblog/connectors/mssql/connector.py` | SQL Server: CDC reads, table reads, `inspect()` |
| `src/pydblog/connectors/mssql/schema.py` | The SQL Server → Arrow type map, and the schema every frame is cast to |
| `src/pydblog/state.py` | Where dump progress is recorded |
| `adls/` | Architecture decisions, with the measurements behind them |
