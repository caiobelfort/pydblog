# One schema for every frame a run yields

- **Date:** 2026-08-13
- **Status:** Accepted
- **Scope:** `connectors/types.py`, `connectors/mssql/`, `dblog.py`

## Context

`DBLog.run()` is a generator. Every frame it yielded came out of a different result
set, and each one's dtypes were inferred independently by `mssql_python`'s
`cursor.arrow()`. Two chunks of a single dump could therefore disagree — an all-null
column typed differently, a decimal widened, a datetime's time unit shifted — and a
consumer stacking them hit a `SchemaError` partway through, after `_checkpoint()` had
already recorded progress for the chunk that broke it.

Worse, the two kinds of frame did not even have the same columns: events carried the
log's metadata ahead of the table's, dump rows carried the table's alone.

The goal: every frame of a run shares one schema, decided from the database's own
metadata, so `pl.concat(dblog.run(...))` works vertically with no reconciliation.

## Decisions

### 1. Cast at Arrow, not at polars

`cursor.arrow()` returns a `pyarrow.Table`, and polars takes its dtypes from that
table's schema. Arrow is therefore where a schema can be *decided* rather than
observed. Casting at the polars layer would mean letting the driver's per-result-set
inference land first and then correcting it.

`connectors/mssql/schema.py` maps SQL Server types to Arrow types and `conform()`
selects and casts the result set to the declared schema before polars sees it.

### 2. Dump rows are stamped into the event schema, marked all-zero

`to_events()` prefixes the five metadata columns onto a dump chunk. `start_lsn`,
`seqval` and `update_mask` are all zeros — a position CDC never issues, so it reads
unambiguously as "taken off the table, not out of the log". It also orders dump rows
below every real event under `(start_lsn, seqval)`, which is the base-image-then-
changes relationship the merge already enforces.

`update_mask` is zeroed *at the width a real mask has* (`ceil(captured / 8)`), not at
the LSN width. **Measured:** `dbo.sales` has 10 captured columns and CDC emits a
2-byte mask; `ceil(10 / 8) = 2`. A mask of another width would not parse.

### 3. Computed columns are read, and read as NULL on both sides

**Measured, against a live server.** The change table *does* have a column for a
persisted computed column:

```
CT COLUMNS: [..., 'quantity', 'unit_price', 'total_amount', 'status', ...]
```

but CDC never fills it. After inserting `quantity = 3, unit_price = 5.00`:

```
CT LAST INSERT (quantity, unit_price, total_amount): (3, 5.00, None)
```

The source table holds `15.00` for that row. So reading the column from the table
would put the computed value on a dump row and NULL on every event for the same row —
one schema carrying two answers, indistinguishable downstream from a real transition
to NULL. `read_table` therefore projects `NULL AS [total_amount]` deliberately, and
`ColumnSpec.computed_definition` carries the formula so a consumer can recompute it.

This matches what SQL Server itself does, rather than departing from it. The previous
code excluded computed columns via an `is_computed = 0` filter and a comment claiming
CDC did not capture them — right outcome, wrong reason, and it hid the formula.

### 4. A chunk is dated from the watermark taken *before* it is read

Each chunk pass takes two watermarks. The chunk is dated from the earlier one:
anything committed after it that touches a chunk row lands inside the window and
supersedes it, so no surviving row is older than that watermark.

It must be a real position. **Measured:**

```
REAL  0000002c000004a80004 -> 2026-08-13 17:24:22.257000
SYNTH 0000002c000004a80005 -> None
```

`_last_lsn` sits one past where the previous window closed — synthetic, never
committed, no row in `cdc.lsn_time_mapping`. Dating from it left every chunk after
the first with no timestamp at all. `run()` now takes `get_max_lsn()` at the top of
each pass for this purpose only; the window still *opens* at `_last_lsn`, because
opening it at the new watermark would skip everything committed in between.

### 5. Captured types come from the change table, not `cdc.captured_columns`

That view records `column_type` but neither precision nor scale, and a decimal needs
both. The change table is an ordinary table, so `cdc.<instance>_CT` is read through
the same `sys.columns` query as the source and the `__$`-prefixed columns dropped.

### 6. Reconciliation is loud, at `inspect()`

`validate()` refuses a spec whose two column lists cannot produce one schema: a
captured column gone from the source, a type that has drifted, an uncaptured or
computed primary key, a business column named after a metadata column, or a type with
no Arrow equivalent. All are cheaper here than mid-dump with progress checkpointed.

**Measured, and it corrected the plan:** an `ALTER COLUMN` type change *is*
propagated to the change table, so it produces no drift to catch. A **dropped** column
is not — it stays in the change table while the source loses it. That is the case the
`dbo.pydblog_drift` fixture exercises.

### 7. Drift is compared as Arrow types, not as type names

The question reconciliation asks is whether the two reads produce the same column,
not whether SQL Server uses the same word for it. `validate()` therefore compares
`arrow_type(source) != arrow_type(captured)`.

**Measured.** A `ROWVERSION` column is reported as two different types by the two
sides:

```
captured column 'row_version' has drifted: the change log records it as
binary(0,0) and dbo.sales now has timestamp(0,0)
```

A change table cannot carry a rowversion of its own — the value is generated per
table — so CDC stores the eight bytes as plain `binary`, while `sys.types` calls the
source column `timestamp`. Comparing names rejected every table with a rowversion
column outright. Comparing Arrow types accepts it, because both map to
`large_binary()`, and still rejects `datetime2(6)` against `datetime2(7)`, which map
to `timestamp("us")` and `timestamp("ns")`.

Note the trap in the name: SQL Server's `timestamp` **is** `rowversion` and has
nothing to do with a clock. It maps to binary in `_FIXED`, which `arrow_type` checks
before the temporal branch, and `_TIMESTAMPS` deliberately excludes it. Typing those
bytes as an instant would corrupt them outright.

## Consequences

- `pl.concat(dblog.run(...))` works vertically. Proven end to end in
  `tests/test_dblog_integration.py` against a real server with writes landing mid-run.
- `TableSpec` is frozen and `business_columns` derived from `captured_columns`, so a
  spec cannot drift from the schema its frames were cast to.
- One extra `get_max_lsn()` round trip per chunk, against a chunk that just read
  `chunk_size` rows.
- A table with a computed column reads that column as NULL. Consumers wanting the
  value derive it from `computed_definition`.
- `validate()`'s type-drift branch is defensive rather than routinely reachable on
  SQL Server, given decision 6's finding. It stays, because a capture instance can be
  created against a schema that later changes in ways this codebase does not enumerate.
