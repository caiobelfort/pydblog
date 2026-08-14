# A TOP pagination option, chosen over fixed key-width chunking

- **Date:** 2026-08-14
- **Status:** Accepted
- **Scope:** `connectors/mssql/connector.py`

## Context

`read_table`'s row cap is enforced with `OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY`. Against
a production table with billions of rows, this pagination was reported as slow, and the
first candidate considered was replacing keyset-by-row-count with fixed key-width
chunks (`WHERE pk >= ? AND pk < ? + width`, advancing by a constant width regardless of
how many rows fall inside it).

## Why fixed key-width was rejected

Measured against a 200,000-row table, sparsified to 1,000 real rows scattered across
the same key range (1 real row per 200 keys — the same shape as the sparse-key case
`_next_chunk`'s docstring already documents):

```
FETCH NEXT (rows)         2 calls    1,000 rows    10.4ms total
Fixed key-width (=1000) 201 calls    1,000 rows    86.3ms total   (8x slower)
Fixed key-width (x200)    2 calls    1,000 rows     4.1ms total   (faster, but
                                                      only because the width was
                                                      hand-tuned to the known 1:200
                                                      sparsity)
```

A fixed width only wins when it happens to match the table's real density. On a table
with billions of rows, sparsity cannot reasonably be measured or kept up to date — it
also is not necessarily uniform across the key range, so a single width can be
simultaneously too wide (oversized result sets in dense regions, risking memory and a
wider log window per chunk) and too narrow (round-trip explosion in sparse regions, as
measured above) on the same table. This is the same reasoning `_next_chunk`'s existing
docstring gives for chunking by row count in the first place; nothing here overturns
it, and switching would be trading a known-good property for a plausible-looking
optimization with no evidence behind it for this table's actual shape.

## What was measured instead

`TOP (?)` is still a row-count cap — safe under any density, exactly like
`FETCH NEXT` — but is older T-SQL syntax that can produce a better query plan than
`OFFSET/FETCH`. Measured on a **dense** 200,000-row table (no sparsity at all, so the
fixed-width risk above does not apply), through two independent paths:

```
Raw SQL, best-of-5:              FETCH 1.786ms/call   TOP 1.365ms/call   (24% faster)
Via MSSQLConnector.read_table:   FETCH 294.6ms total  TOP 215.3ms total  (27% faster)
```

Same call count, same rows returned, in both measurements — the difference is
per-call overhead in `OFFSET/FETCH`'s execution plan that `TOP` does not pay.

## Decision

Add `pagination` as a connector option, default `"fetch"` (unchanged behavior),
selectable as `"top"`:

```python
MSSQLConnector(..., pagination="top")
DBLog(..., pagination="top")   # passed through **kwargs
```

`_next_chunk`'s keyset logic is untouched — `start_pk` still advances from the last
row's leading key, chunking is still by row count. Only the SQL that enforces the cap
changes: `TOP (?)` sits right after `SELECT`, so its placeholder binds first,
regardless of where `start_pk`/`end_pk` land in the `WHERE` clause.

Validated eagerly at construction (`ValueError` for anything other than `"fetch"` or
`"top"`), so a typo surfaces immediately rather than on the first query.

## Consequences

- Existing behavior is the default; opting into `TOP` is a one-kwarg change.
- The ~24-27% per-chunk gain is measured on a dense table only. Sparsity was
  deliberately left out of scope for this change — see above.
- Two independent measurements (raw SQL, and through `read_table` itself) agreed,
  which is what makes this a decision rather than a hunch from one benchmark shape.
