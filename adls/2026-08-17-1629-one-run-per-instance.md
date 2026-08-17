# The run is settled at construction, so an instance is one run

- **Date:** 2026-08-17
- **Status:** Accepted
- **Scope:** `dblog.py`, and the consumer contract

## Context

`fetch()` took `schema`, `table`, `dump` and `from_lsn` on every call, left over from
when it was a generator called once. Once it returned one batch per call, repeating
them per batch meant repeating what could not vary — and every one of them raised the
question of what a caller changing it halfway through meant. The answers were all
awkward:

- `from_lsn` was honoured on the first call and silently ignored afterwards. A caller
  passing a new one got no error and no effect.
- A changed `dump` or `table` abandoned the run in progress and seeded a new one, which
  no caller asked for and none could easily have wanted.
- `_run` existed only to tell those cases apart: a tuple of the three, compared on
  every call, to decide whether seeding had already happened.

The third of these produced a real bug. `dump_done` reported on whichever dump was last
seeded, and a dump is not seeded until `fetch()` names it, so a finished dump left the
flag True for the next one. `while not dblog.dump_done:` — the obvious shape, and the
one this repo's own README documented — then never entered its loop. It emptied the
integration fixtures, which shared one module-scoped instance across two dumps, and
surfaced three tests away from the cause as `pl.concat` refusing an empty list.

## Decision

The run identity moves to `__init__`. `fetch()` takes no arguments.

```python
with DBLog(..., schema="dbo", table="sales", dump="sales-backfill") as dblog:
    while (frame := dblog.fetch()) is not None:
        write(frame)
        dblog.commit()
```

An instance is one run. A second table, or the same table under a second dump name, is
a second `DBLog` — the integration fixture became a factory for exactly that reason.

`_run` is gone, replaced by a plain `_started` flag: seeding is still lazy, on the
first fetch, because `__init__` doing I/O would connect outside the `with` block that
exists to own the connection.

Two checks moved to `__init__` along with the arguments they check — a blank `dump`,
and neither `dump` nor `from_lsn`. Both are decidable without the source, so they now
raise before the connector is even built, matching how `chunk_size` and the `pagination`
option are already validated eagerly.

## Consequences

- **Breaking**, and loudly so: `fetch("dbo", "sales", dump=...)` is a `TypeError`, not
  a silent change of meaning.
- `while not dblog.dump_done:` is safe to write now, and the README says so plainly
  instead of warning about ordering. The flag starts False on every instance and only
  the run's own progress moves it.
- One `DBLog` can no longer be reused across tables. That is the point, but it is worth
  saying: callers reading many tables build many instances, and each opens its own
  connection.
- Errors that need the source — no capture instance, a dump pointed at another table,
  an LSN aged out — still surface on the first `fetch()`, not at construction. Argument
  validation and source validation happen in different places, and the docstrings say
  which is which.
