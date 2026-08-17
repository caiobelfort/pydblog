# `fetch()` returns one batch per call instead of generating them

- **Date:** 2026-08-17
- **Status:** Accepted
- **Scope:** `dblog.py`, and the consumer contract

## Context

The method was called `run()` and was a generator; it is `fetch()` now, renamed in the
same change because "run" reads like something that goes away and does the whole job,
which is exactly what it stopped doing. Laziness bought it nothing that mattered and
cost two things that did.

The first is that a generator holds a suspended frame of execution, so the state a
resume depends on lived half on the instance and half in that frame. Driving it
partway and abandoning it — `next(stream)` then `stream.close()`, which is what an
interrupted consumer does — left the run reachable only through an object that could
not be restarted.

The second is that laziness deferred the errors. `fetch()` validated its arguments and
inspected the table only once iterated, so a caller that built a run and did not
consume it got no signal that the dump name was blank or the LSN had aged out. The
old docstring had to say so out loud: "nothing is read, and none of the errors below
are raised, until it is iterated."

Neither bought streaming, which is what laziness is usually for. Batches were already
bounded by `chunk_size` and already handed over one at a time.

## Decision

`fetch()` does one unit of work and returns one frame, or None when there is nothing to
read:

```python
while (frame := dblog.fetch("dbo", "sales", dump="sales-backfill")) is not None:
    write(frame)
    dblog.commit()
```

Seeding stays a per-run cost rather than a per-batch one: the first call runs
`_start()` — `inspect()`, `get_min_lsn()`, a store read — and later calls for the same
`(schema, table, dump)` skip it. The tuple is tracked in `_run`. Calling with a
different table or dump abandons the run in progress and seeds the new one, which is
the only reading of a changed argument that is not a silent no-op. `from_lsn` is
therefore honoured on the first call only.

Nothing about the algorithm's ordering changed — watermark, chunk, watermark, await,
window — only that one pass now returns rather than yields, and the `while not
self._dump_done` loop around it belongs to the caller.

## What None means, and why `dump_done` exists

None means "nothing right now", not "never again": the table is walked and the log is
caught up *as of this call*. It ends a loop, not the run.

Past the end of the table a batch is a window on its own, so `while fetch() is not None`
slides from dumping into tailing without a seam. That is right for a consumer that
wants both and wrong for a backfill that wants to stop, and the generator's implicit
`return` at end-of-dump used to draw that line. `dump_done` draws it explicitly
instead:

```python
while True:
    frame = dblog.fetch("dbo", "sales", dump="sales-backfill")
    ...
    if dblog.dump_done:
        break
```

It also answers the question the caller could not previously ask, which the explicit
`commit()` had just made pressing: whether the dump is finished, and so whether the
trailing `commit()` that records `done` is worth making.

The order matters, and the obvious shape is the wrong one. `dump_done` reports on the
run that is *seeded*, and a dump is not seeded until `fetch()` names it — `_start()` is
what resets the flag. So `while not dblog.dump_done:` reads the previous dump's state
on the way in, and on a reused `DBLog` the loop never runs at all. That is not
hypothetical: it emptied the integration fixtures, which share one module-scoped
instance across `concat-proof` and `race-proof`, and surfaced as `pl.concat` refusing
an empty list. Fetch first, ask second.

## Consequences

- **Breaking.** `for frame in dblog.fetch(...)` and `list(dblog.fetch(...))` both stop
  working — the latter silently, since iterating a `DataFrame` yields its columns.
  Anything holding a `fetch()` result as an iterator has to become a loop over calls.
- Errors now surface on the call that causes them. The tests that asserted the
  opposite (`test_run_does_nothing_until_it_is_iterated`) assert eagerness instead.
- `_merge_chunk`'s `if not merged.is_empty()` guard went away. It was unreachable once
  the window was folded into the merged frame (`457ea79`) — a non-empty window makes
  the frame non-empty, and an empty one leaves the chunk unsuperseded — and keeping it
  would have meant returning None mid-dump, which truncates a caller's loop.
- The dump→tail transition is no longer a hard stop, so a `while fetch() is not None`
  loop against a table under continuous writes does not terminate on its own. That is
  the caller's loop to bound, and `dump_done` is the bound for the common case.
