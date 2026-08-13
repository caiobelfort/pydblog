# AGENTS.md

Notes for an agent working in this repo. `README.md` covers running it; this covers
what will bite you.

## What this is

The DBLog algorithm over SQL Server CDC. `DBLog.run()` is a generator that interleaves
a chunked table dump with the change log, yielding polars DataFrames.

`DBLog` owns the algorithm; the connector owns the database. `DBLog._connector` is
typed as the `SourceConnector` Protocol, not as `MSSQLConnector`, so the algorithm can
only reach for the primitives every source must provide. Adding SQL Server specifics
to `dblog.py` breaks that on purpose — put them behind a Protocol method instead.

## The invariant everything rests on

**Every frame a run yields has the same schema.** Change events and dump rows alike.
Break this and the failure surfaces at the consumer's `pl.concat`, long after the
chunk that caused it was checkpointed.

It holds because:

- Dtypes are decided at **Arrow**, not polars. `cursor.arrow()` is what determines the
  polars dtype, so `connectors/mssql/schema.py` casts the arrow table before
  `DataFrame()` ever sees it. Never "fix up" dtypes after construction.
- Both read paths go through `conform(table, schema, context)`.
- `to_events()` stamps dump chunks into the event schema.
- `TableSpec` is **frozen**, and `business_columns` is a **property** derived from
  `captured_columns` — not a field. `model_copy(update={"business_columns": ...})`
  silently does nothing; update `captured_columns` instead.

## Things that are counter-intuitive and were measured

Do not "correct" these from memory — each was checked against a live server, and the
evidence is in `adls/2026-08-13-1730-one-schema-per-run.md`.

- **CDC creates a column for a persisted computed column and never fills it.** The
  change table has `total_amount`; every event carries NULL there. So `read_table`
  projects `NULL AS [total_amount]` on purpose. Reading the real value would make dump
  rows and events disagree about the same row.
- **`cdc.captured_columns` has no precision or scale.** Captured types are read from
  `cdc.<instance>_CT` via the same `sys.columns` query as the source table.
- **`ALTER COLUMN` is propagated to the change table**; a **dropped** column is not.
  Only the latter produces drift `inspect()` can catch.
- **SQL Server's `timestamp` is a `ROWVERSION`, not a time.** Eight opaque bytes. It
  maps to binary, and `_FIXED` is checked before the temporal branch so it can never
  fall through to `pa.timestamp`. Do not "fix" this.
- **A rowversion is `timestamp` on the source and `binary` in the change table** — a
  change table cannot have a rowversion of its own. This is why `validate()` compares
  `arrow_type(...)` and not type names: comparing names rejects every table that has
  one.
- **`_last_lsn` is not a real log position.** It sits one past where the last window
  closed, so `fn_cdc_map_lsn_to_time` returns NULL for it. Chunks are dated from a
  `get_max_lsn()` taken at the top of each pass. That watermark is for dating only —
  the window must still open at `_last_lsn`, or events in between are skipped.

## Conventions

- `uv` for everything. Dev deps in the `dev` dependency group.
- Google-style docstrings on public functions; say *why*, not *what*.
- Type hints everywhere. `uv run pyrefly check src` must report 0 errors.
- pytest with fixtures, `@pytest.mark.parametrize` for edge cases, `pytest.raises` for
  exceptions. No unittest.
- **Tests first.** Write the test, watch it fail, then implement.
- Prefer functions and modules over classes; immutable data; explicit over implicit.
- Record architecture decisions in `adls/`, filename prefixed with a timestamp.

## Test layout

| File | Needs Docker | Covers |
| --- | --- | --- |
| `tests/test_types.py` | no | `ColumnSpec` equality/hash, `TableSpec` derivation |
| `tests/test_mssql_schema.py` | no | The type map, `conform`, `validate` |
| `tests/test_dblog.py` | no | The algorithm, against `StubConnector` |
| `tests/test_state.py`, `test_log.py` | no | Progress store, logging |
| `tests/test_mssql.py` | mostly | The connector against a real server |
| `tests/test_dblog_integration.py` | yes | That a whole run actually concatenates |

`tests/test_mssql.py` mixes both: query-building tests run without a container, the
rest are `@pytest.mark.integration`.

The `StubConnector` in `tests/test_dblog.py` implements the whole Protocol. Add a
Protocol method and you must add it there too.

## Gotchas

- Stop the dev-compose SQL Server before running integration tests. Two instances OOM
  each other; you get exit 137 and containers exited 255, which reads like anything
  but a memory problem.
- `polars` treats `bytes` as a sequence in `Series.eq()`. Comparing an LSN column
  against `bytes(10)` raises a `ShapeError` about mismatched lengths. Compare
  `.to_list()` instead.
- CDC's capture job runs on its own schedule. A test that writes and then expects an
  event must wait — `conftest.wait_for_cdc` — or it passes alone and fails in a suite.
