# AGENTS.md

Notes for an agent working in this repo. `README.md` covers running it; this covers
what will bite you.

## What this is

The DBLog algorithm over SQL Server CDC. `DBLog.fetch()` interleaves a chunked table
dump with the change log, returning one polars DataFrame per call.

`DBLog` owns the algorithm; the connector owns the database. `DBLog._connector` is
typed as the `SourceConnector` Protocol, not as `MSSQLConnector`, so the algorithm can
only reach for the primitives every source must provide. Adding SQL Server specifics
to `dblog.py` breaks that on purpose — put them behind a Protocol method instead.
`dblog.py` imports no connector module at all; `build_connector()` in
`connectors/base.py` is the only construction point.

## Commands

```bash
uv sync                                   # install, including the dev group

uv run pytest -m "not integration"        # fast suite, no Docker (~0.3s)
uv run pytest                             # everything, starts SQL Server via testcontainers
uv run pytest tests/test_dblog.py         # one file
uv run pytest tests/test_types.py::test_signature_is_the_type_alone   # one test
uv run pytest -k "rowversion"             # by keyword, across files
uv run pytest -q -s                       # -s to see print/log output from a probe

uv run pyrefly check src                  # a global uv tool, not a project dep:
                                          # uv tool install pyrefly
```

No build step, no linter beyond pyrefly, and `[project.scripts]` is empty — this is a
library with no CLI yet.

Integration tests are gated on Docker; without it they skip rather than fail. Changing
`sql/mssql/01_setup.sql` only takes effect on a fresh container, which each pytest
session creates. The script is idempotent.

## How the pieces fit

Each is unremarkable alone; the shape only makes sense together.

**The run loop** (`DBLog.fetch()`) is the algorithm, and its ordering is load-bearing:

```
one call, one batch:
  seed once             # _start(): inspect + store read, only on the first fetch
  _window_low = get_max_lsn()      # before-watermark, for dating this chunk only
  chunk  = _next_chunk()           # keyset page from _chunk_key
  window = _read_window()          # (_last_lsn, get_max_lsn()], then _last_lsn = high+1
  return _merge_chunk(chunk, window)  # one frame: window events, then _supersede
                                      # (anti-join) + to_events (stamp) of the chunk
                                      # caller calls commit() once it has written it
```

`fetch()` returns one frame, or None when the table is walked and the log is caught up.
It is not a generator and holds nothing back: the read happens in the call. Past the
end of the table a batch is a window alone, so a plain `while fetch() is not None` loop
slides from dumping into tailing — `dump_done` is there for a caller that wants to
stop at the end of the table instead.

**An instance is one run.** `schema`, `table`, `dump` and `from_lsn` are constructor
arguments, so `fetch()` takes none: they identify the run, not a batch of it. A second
table or a second dump name is a second `DBLog` — which is why the integration fixture
is a factory rather than one shared instance.

That is also what makes `while not dblog.dump_done:` safe to write. It was not, back
when `fetch()` took the dump name per call: the flag described whichever dump was last
seeded, so a finished dump left it True and the next dump's loop never ran at all.
`test_a_finished_dump_does_not_bound_a_dump_on_another_instance` pins that it starts
False per instance.

`dump_done` is `self._dump is None or self._dump_done`, and the first half is not
cosmetic: a log-only run has no table walk, so reporting False would leave that same
loop spinning on a condition nothing can satisfy — one `get_max_lsn` round trip per
turn, forever. Vacuous truth makes it terminate instead, at the cost of doing nothing,
which is the right answer to "walk the table" when there is no dump.

The window closes *after* the chunk scan, so anything committed during the scan lands
inside it and the chunk cannot carry a stale row the window does not also correct.
Windows are half-open via `increment_lsn`, because the CDC read is inclusive on both
bounds.

**The schema pipeline** spans `connectors/types.py`, `connectors/mssql/connector.py`
and `connectors/mssql/schema.py`:

```
inspect()  → reads source sys.columns + cdc.<instance>_CT sys.columns
           → reconciles them (validate) → a frozen TableSpec
           ↓
row_schema(spec) / event_schema(spec)   → pyarrow.Schema
           ↓
read_table / read_event_log → cur.arrow() → conform(...) → DataFrame
to_events(rows, spec, ts)   → stamps a dump chunk into the event schema
```

**Resume** (`state.py`). `DumpState` writes `chunk_key` and `last_lsn` as a unit —
either alone would leave rows that neither the remaining chunks nor the remaining log
would deliver. `JsonFileStore` writes to a temp file and `os.replace`s it, so an
interrupted write leaves the previous state intact. Injecting a `StateStore` is how
tests avoid the filesystem.

Nothing is written until the caller calls `DBLog.commit()`. The run loop deliberately
does not do it: receiving a frame is not the same as having written it, and a position
recorded on the strength of receipt puts the rows behind it out of reach when the
caller's own write then fails. Uncommitted frames are re-read, which is the safe
direction to fail in — at-least-once is the guarantee the algorithm already makes.

An `LSN` is `bytes` — 10 bytes, big-endian, so byte order is numeric order and plain
comparison works.

## Watermarks, and why a chunk pass is slow

Each chunk is bracketed the way the paper brackets it (§IV-C), but with timestamps
instead of writes:

```
low  = watermark()          # the source's clock, before the scan
chunk = _next_chunk()
high = watermark()          # after the scan
await_watermark(high)       # ← the barrier; ~5s, and not optional
window = _read_window()
```

**The wait is the algorithm, not an optimisation to remove.** `get_max_lsn()` reports
how far the *capture job* has read, not how far the database has committed. Measured:
a just-committed change was outside it **5 times out of 5** — the value had not moved
at all. Closing a window on it let a row updated during a chunk scan be emitted with
its pre-update value, uncorrected for the rest of the run. That is what
`tests/test_dblog_integration.py::test_a_write_during_a_chunk_scan_does_not_escape_as_a_stale_row`
pins.

The paper writes to a watermark table so the write can be *observed* coming back out
of the log. The write is not the point — waiting for it is — so a timestamp plus a
way to ask "has the log consumer passed this?" does the same job with no write
permission and no extra table.

`await_watermark` accepts two signals, and needs both:

- **`start_time` of a scan later than the mark** — the log consumer began a pass after
  the watermark. This is the signal while the table is being written to.
- **an *empty* scan ending after the mark** — the job read to the end of the log and
  found nothing, so it is caught up. This is the signal while it is idle, which is a
  dump's normal state since a dump only reads. Measured: consecutive empty scans reuse
  one session row, so `start_time` **freezes** and only `end_time` moves. Waiting on
  `start_time` alone hangs a dump.

Two dead ends, both measured, so nobody re-treads them: `log_end_lsn` from
`sys.dm_db_log_stats` never crosses `get_max_lsn()` (it counts every log record, while
`get_max_lsn()` only moves for CDC transactions), and the scan session's `end_lsn` is
the same value as `get_max_lsn()` and is zeroed on empty scans.

Cost is one capture polling interval per chunk — 5s by default, tunable with
`sp_cdc_change_job @pollinginterval`. A dump of N chunks pays roughly N × that.

## The invariant everything rests on

**Every frame a run returns has the same schema.** Change events and dump rows alike.
Break this and the failure surfaces at the consumer's `pl.concat`, long after the
chunk that caused it was committed.

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
- **Trunk-based:** commit directly to `main`. Do not create feature branches.
- Record architecture decisions in `adls/`, filename prefixed with a timestamp, and
  record what was *measured*, not only what was decided. The existing ADR is written
  that way, which is why its surprising findings survived review.
- This file is the only agent-facing doc. Do not add a `CLAUDE.md` or equivalent —
  put agent guidance here.

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
- `.claude/AUDIT.md` is a findings document from an earlier review. Several of its
  findings (F2, F3, F4, F19) are closed; check against current code before acting on
  any of them.
