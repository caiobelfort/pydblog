# pydblog

Streams a SQL Server table's changes, optionally interleaved with a chunked dump of
its rows, using the [DBLog](https://arxiv.org/abs/2010.12597) algorithm over CDC.

One call reads one batch and hands back two things: the batch as a polars DataFrame,
and the state to carry on from. Every frame shares one schema — change events and dump
rows alike — so no frame needs reconciling against another. You write the frame and keep
the state, and the state is what the next call resumes from:

```python
from pydblog.connectors.base import build_connector
from pydblog.dblog import CdcRetentionExpiredError, SchemaChangedError, dblog

source = build_connector(
    source_type="mssql",
    host="localhost", port="1433",
    user="sa", password="...", database="dblog_lab",
)

with source:
    state = load_my_state()          # None the first time
    while True:
        try:
            result = dblog(source, "dbo", "sales", state=state, chunk_size=1000)
        except (CdcRetentionExpiredError, SchemaChangedError):
            state = None             # dump the table again
            continue

        if result.frame is not None:
            result.frame.write_delta("s3://lake/sales", mode="append")

        save_my_state(result.state)   # ideally in the same transaction as that write
        state = result.state

        if result.frame is None:
            break
```

**The state controls everything.** With none, the run opens at the present and dumps the
table. With one, the batch carries on from where that state left off. With one whose
`dump_done` is set — which is what a state becomes once the table is walked — the batch
is a window of the log alone, so the same call slides from dumping into tailing with
nothing retrieved from anywhere. To stop at the end of the table instead — a backfill
rather than a tail — loop on `state.dump_done`:

```python
state = None
while state is None or not state.dump_done:
    result = dblog(source, "dbo", "sales", state=state, chunk_size=1000)
    state = result.state
    if result.frame is not None:
        result.frame.write_delta("s3://lake/sales", mode="append")
        save_my_state(state)
```

Keep the state even off a result whose frame is `None`: an empty window still moved the
log position, and dropping it makes the next call re-scan a widening range.

Nothing is buffered and nothing is lazy: the batch is read by the call, and its size is
bounded by `chunk_size` however large the table is. Rows that came off the table rather
than out of the log are marked with an all-zero `start_lsn` and `operation = 0`, a
position CDC never issues.

**Nothing is written anywhere but by you.** That is not a convenience — it is what stops
a recorded position from getting ahead of the data it describes. The frame and the state
come back together, so you can persist both or neither; a frame you received but never
saved is read again by the next call given the older state, which is the at-least-once
guarantee the algorithm already makes. Where the state lives, and when to dump the table
again, are yours: `RunState` is a pydantic model, so `model_dump_json()` and
`RunState.model_validate_json()` are all a file or a row needs.

The state carries the table's spec as `inspect()` described it, which is what settles
the schema every frame shares. `dblog()` re-reads that spec once a day by default
(`inspect_every`) and stops the run with `SchemaChangedError` if the capture instance
has started projecting something else, rather than letting frames that no longer stack
reach your `concat`.

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
| `src/pydblog/dblog.py` | The algorithm: chunk, window, supersede, merge |
| `src/pydblog/connectors/base.py` | The `SourceConnector` Protocol every source implements |
| `src/pydblog/connectors/mssql/connector.py` | SQL Server: CDC reads, table reads, `inspect()` |
| `src/pydblog/connectors/mssql/schema.py` | The SQL Server → Arrow type map, and the schema every frame is cast to |
| `src/pydblog/state.py` | `RunState` and `BatchResult` — what a run carries, and what a call hands back |
| `adls/` | Architecture decisions, with the measurements behind them |
