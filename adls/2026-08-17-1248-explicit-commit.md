# Progress is committed by the caller, not by the run loop

- **Date:** 2026-08-17
- **Status:** Accepted
- **Scope:** `dblog.py`, and the consumer contract

## Context

`run()` recorded progress itself, calling `_checkpoint()` after the yields on both
paths. The comment defending its position read:

```
# After the yields, not before: this line only runs once the consumer
# comes back for more, which is the only evidence there is that it
# took what the iteration produced.
```

The reasoning holds for the failure it was written against — a crash *inside* the
generator — but "the consumer came back for more" is evidence the frame was
**received**, not that it was **written**. A consumer whose own write fails has
already had the position advanced past the frame, and nothing brings those rows
back: the chunk sits behind `chunk_key` and the events behind `last_lsn`. Both are
the resume floor, so the next run starts after data that was never persisted.

That is the one failure mode the algorithm is supposed to rule out. Every other
part of it is built for at-least-once — the window supersedes the chunk precisely
so a row delivered twice is harmless — and then the checkpoint quietly made loss
possible.

## Decision

`_checkpoint()` becomes public as `commit()`, and `run()` stops calling it. The
body is unchanged: it was already a pure snapshot of in-memory run state.

```python
for frame in dblog.run("dbo", "sales", dump="sales-backfill"):
    frame.write_delta(...)
    dblog.commit()

dblog.commit()   # once more: records the dump as finished
```

No new bookkeeping was needed to make this correct. Both cursors already advance
*before* the frame is yielded — `_chunk_key` in `_next_chunk`, `_last_lsn` in
`_read_window` — so while the consumer holds frame N the in-memory state already
points one past it, which is exactly the resume position for "frame N is safe".
`commit()` records where the run *is*, not which frame it was handed, so
committing per frame and per batch are both sound; the only difference is how much
is re-read after a failure.

It is a no-op before a run starts and for a run with no dump (there is no name to
key progress under), so consumer code handling both kinds of run can call it
unconditionally.

## Why not an `autocommit` flag

Rejected: any automatic write can outrun the caller's durable write, so leaving one
in the default path preserves the bug for everyone who does not know to opt out.
A flag would also mean two code paths and two sets of tests for a behaviour whose
safe setting is the only one worth having.

## Consequences

- **Breaking.** The old README idiom, `frames = list(dblog.run(...))`, now records
  nothing — correctly, since `list()` offers nowhere to write from. The README shows
  a per-frame loop instead.
- A run whose frames are never committed starts over. That is the intended
  direction to fail in, but it is silent: no warning fires for an uncommitted run,
  on the grounds that a caller deliberately discarding frames is legitimate.
- `done` needs the trailing `commit()`. A dump ends on an iteration that yields
  nothing — the page came back empty — so the flag has no frame to ride out on.
  Skipping that last call costs the next run one empty `read_table` to rediscover
  the end of the table, and nothing else.
- `tests/test_dblog.py::test_records_progress_only_once_the_frames_are_taken` was
  the old contract stated as a test; it is replaced by
  `test_a_run_records_nothing_on_its_own` and
  `test_an_uncommitted_run_is_read_again_from_the_start`.
