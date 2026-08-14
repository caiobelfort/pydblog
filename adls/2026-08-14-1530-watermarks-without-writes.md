# Watermarks without writing to the source

- **Date:** 2026-08-14
- **Status:** Accepted
- **Scope:** `dblog.py`, `connectors/base.py`, `connectors/mssql/connector.py`
- **Supersedes:** the claim in `.claude/AUDIT.md` that the watermark substitution was
  sound (F0)

## Context

The DBLog paper brackets each chunk SELECT with a low and a high watermark *written*
to a watermark table, and waits until the high watermark is observed coming back out
of the log before emitting the chunk (§IV-C). That is what makes the emitted chunk
free of stale rows.

This implementation wrote nothing and closed the window on
`sys.fn_cdc_get_max_lsn()`. The justification on record was that "since the capture
job lags real commits, that high bound is at or past anything committed during the
scan." That is a non-sequitur — lagging puts the bound *behind* — and it was measured
to be false:

- Commit an UPDATE, then read `get_max_lsn()` immediately: **5 of 5 attempts**, the
  value had not moved at all, and the change's real LSN was strictly greater.
- Injecting an UPDATE between the chunk read and the window read, the dump emitted the
  **pre-update** value, and nothing later in that run corrected it. (A subsequent run
  did, so no data was lost — the guarantee was weaker, not broken.)

## Decision

Bracket each chunk the way the paper does, but use a **timestamp** as the watermark
instead of a written row.

```
low  = watermark()          # the source's clock
chunk = _next_chunk()
high = watermark()
await_watermark(high)       # the barrier
window = _read_window()
```

Two new `SourceConnector` primitives: `watermark()` returns the source's own clock,
and `await_watermark(mark)` blocks until the source's log consumer has processed
everything committed by `mark`. How a source answers that is its own business.

The write was never the point — being able to *wait* for it was. A source that can
report where its log consumer has reached does not need the write, and keeps the
read-only property the audit calls a chief claim: no watermark table, no write
permission, only `VIEW DATABASE STATE`.

The low watermark also replaces `_window_low` as the chunk's `commit_timestamp`,
removing a `map_lsn_to_timestamp` round trip per chunk.

## What the barrier is, and the two things it is not

On SQL Server, `await_watermark` polls `sys.dm_cdc_log_scan_sessions` and accepts
either of two signals. It needs both, which is not obvious:

- **A scan whose `start_time` is later than the mark.** The job resumes where it
  stopped and reads forward, so such a scan covers everything committed before the
  mark. This is the signal while the table is being written to.
- **An *empty* scan ending after the mark.** The job read to the end of the log and
  found nothing, so it is caught up as of then.

The second is not redundant. **Measured:** on an idle database, consecutive empty
scans reuse one session row, so `start_time` freezes while `end_time` advances every
5s and `empty_scan_count` ticks up:

```
[ 3] sessions=2  start=15:22:23.320  end=15:22:23.320  empty=1
[ 5] sessions=2  start=15:22:23.320  end=15:22:28.327  empty=2
[ 8] sessions=2  start=15:22:23.320  end=15:22:33.327  empty=3
```

A dump only reads, so an idle database is its normal state. Waiting on `start_time`
alone hung every chunk until the timeout — which is exactly how the first
implementation of this failed.

Two dead ends, both measured, recorded so they are not re-tried:

- **`sys.dm_db_log_stats.log_end_lsn`** is the true end of log and in the same LSN
  space, but `get_max_lsn()` never reaches it: `log_end_lsn` advances on every log
  record while `get_max_lsn()` only moves for CDC-relevant transactions. Polled 60s,
  never met. A production wait on this would hang forever.
- **`sys.dm_cdc_log_scan_sessions.end_lsn`** is the same value as `get_max_lsn()` and
  is **zeroed on empty scans**.

## Consequences

- An emitted chunk no longer contains a row changed during its own scan. A completed
  run reconstructs to correct state on its own, which is what the paper is for.
- **A chunk pass now costs one capture polling interval — 5s by default.** A 1000-chunk
  dump gains roughly that many seconds. Tunable with `sp_cdc_change_job
  @pollinginterval`; the lab is left at the default so measurements match a stock
  install. This is the price of the guarantee and is the same price the paper's
  written watermarks would carry, since neither makes the capture job run sooner.
- `await_watermark` raises `TimeoutError` rather than hanging when the capture job is
  not running, which is otherwise indistinguishable from an idle database.
- The `_supersede` anti-join is now genuinely load-bearing rather than an
  optimisation: the window it works against is provably complete.
