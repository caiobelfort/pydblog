# pydblog audit

Every function read against the Netflix DBLog paper ("DBLog: A Watermark Based Change
Data Capture Design", Andreas Andreakis & Ioannis Papapanagiotou) and against the
project's Python rules. Findings are ranked and anchored to `file:line`.

Scope: `src/pydblog/` and `tests/` at `aac25e6`. The previous audit at the repo root
covered a since-reverted change set and has been removed; findings that survived into
the current code are restated here with fresh anchors, and the ones the rewrite closed
are listed at the end so they are not re-raised.

State of the tree: 204 tests (165 unit, 39 integration), `pyrefly check` clean,
Python floor 3.12.

---

## What the implementation gets right

Worth stating plainly, because most of it is load-bearing and easy to break later.

**The watermark substitution is sound.** The paper brackets each chunk with a low and
a high watermark *written* to the source (§IV-C). This implementation writes nothing:
it takes `_last_lsn` — where the previous window closed, a position the log had
reached before this scan began — as the low bound, and `sys.fn_cdc_get_max_lsn()`
*after* the scan returns as the high bound (`dblog.py:273-274`). Since the capture job
lags real commits, that high bound is at or past anything committed during the scan,
so the window is a **superset** of the paper's ambiguity window. Over-inclusion costs
re-delivery and nothing else. The ordering is asserted directly rather than inferred
from results (`test_dblog.py:764`), which is the right thing to pin.

**No locks, no writes to the source.** The paper's chief claim is that a dump needs
neither (§I). Nothing here takes a lock, opens a transaction, or needs INSERT anywhere.
A least-privilege reader suffices.

**Chunking is by row count, per §IV-B.** `read_table(start_pk=key, limit=chunk_size)`
with no upper bound, advancing to the last row's key + 1 (`dblog.py:367-377`). Chunk
size is therefore independent of key density. Verified against a lab table whose
identity cache had jumped across restarts — 8 rows spanning keys 1..2004 read in 4
chunks, where key-width chunking would have planned ~1002 mostly-empty intervals.

**The merge is an anti-join on the whole primary key**, order-preserving
(`dblog.py:326-333`). `maintain_order="left"` is not decorative: polars defaults joins
to unordered, and chunks arrive sorted by key.

**Checkpoint-after-yield gives at-least-once.** `_checkpoint()` sits after the yields
(`dblog.py:288`), so it runs only when the consumer comes back for more — the only
evidence available that it took what the iteration produced. A crash costs one chunk
re-delivered rather than one chunk lost.

**Half-open windows.** `increment_lsn` past the high bound (`dblog.py:458`), because
`fn_cdc_get_all_changes` is inclusive on both sides.

---

## High

**F1 — A primary-key-changing UPDATE orphans a row, permanently.**
`read_event_log` reads in `N'all'` mode (`mssql.py:249`), which returns after-images
only. An `UPDATE` that moves a row's primary key emits one event carrying the *new*
key and nothing at all for the old one. A consumer keyed by primary key therefore
keeps the old key forever, with no later event able to retract it — the row was never
deleted, so no delete is ever captured.

*Failure:* `UPDATE dbo.sales SET sale_id = 9001 WHERE sale_id = 5` leaves the consumer
holding both 5 and 9001 indefinitely.

Options: `N'all update old'` and synthesise a delete for the before-image key; or
refuse such tables at `inspect()`; or document the limitation the way Debezium's SQL
Server connector does. Not detectable at runtime as things stand.

**F2 — `business_columns` comes from `sys.columns`, not `cdc.captured_columns`.**
`inspect` reads every non-computed column of the base table (`mssql.py:188-199`), and
`read_event_log` then names all of them against the change table (`mssql.py:245`). CDC
capture instances can be created over a *subset* via `@captured_column_list`. When
they are, the read names a column the change table does not have and fails with
"Invalid column name" — at read time, deep in a run, not at `inspect()`.

The reverse direction is live too and currently benign: in the lab, `total_amount` is
a persisted computed column that CDC *does* capture but `inspect` excludes, so events
silently carry one fewer column than the change table holds. Confirmed by querying
`cdc.captured_columns` directly against the running lab.

A third consequence: if a primary key column is itself computed, `inspect` drops it
from `business_columns`, so `_merge_chunk`'s `window.select(self._spec.pk_columns)`
(`dblog.py:326`) raises `ColumnNotFound` and the whole dump dies.

The paper solves this with a schema store (§IV-F). The minimum here is to read the
captured column list and reconcile, loudly, at `inspect()`.

**F3 — A business column named `operation` breaks both the read and the consumer
contract.** `read_event_log` aliases the four metadata columns to bare names —
`start_lsn`, `seqval`, `operation`, `update_mask` (`mssql.py:239-244`) — then appends
the business columns. A table with a column of any of those names produces two columns
of the same name and polars raises `DuplicateError`.

This is worse than it was before the algorithm existed. `run()`'s documented contract
(`dblog.py:250-253`) tells consumers that event frames and row frames are told apart by
the presence of the log's metadata columns. A table with an `operation` column makes
that heuristic wrong even if the read is fixed. Prefixing the metadata aliases (`__op`,
`__lsn`) would settle both.

**F4 — No integration test covers the algorithm at all.**
83 `DBLog` tests and 14 `state` tests, every one against `StubConnector`/`StubStore`;
all 39 integration tests are `MSSQLConnector`'s (`test_mssql.py`). `grep -c
'@pytest.mark.integration' tests/test_dblog.py` → 0.

The property that cannot be stubbed is the one the paper exists for: that the window
genuinely covers the chunk scan when writes land *during* it. A stub answers whatever
the test scripted. The lab runs done by hand during development are not committed, so
nothing reproduces them.

Worth writing: a PK-changing UPDATE inside a window (F1); a dump interrupted and
resumed against real CDC; concurrent writes during a chunk scan, asserting the merge
drops the stale copy; a table whose CDC was enabled seconds earlier.

**F5 — `DBLog` is single-use, and nothing says so.**
`run()` is a generator that mutates instance state (`_last_lsn`, `_chunk_key`,
`_dump_done`, `_spec`). Two overlapping `run()` calls on one instance — two tables,
or a dump and a drain — interleave their writes to that state and silently corrupt
both. There is no guard and no note in the docstring.

*Failure:* `a = dblog.run("dbo", "sales", dump="x"); b = dblog.run("dbo", "orders")`,
then advancing both, reads `orders` positions into `sales`'s chunk key.

Either refuse a second concurrent run, or move run state into a per-run object. The
project's own rules prefer stateless (`SKILL.md:39-44`).

---

## Medium

**F6 — The class docstring contradicts the code.** `dblog.py:41-42`: "Run state lives
on the instance and only in memory: a run that dies restarts from the beginning."
Resumability landed; that sentence is exactly backwards now.

**F7 — Stale rationale in `read_table`.** `mssql.py:427-430`: "sparse ranges are
expected, since chunk_size is a key width and not a row count". Chunking is by row
count. The comment argues for a design that was replaced.

**F8 — `read_pk_range` and `read_table`'s `end_pk` are dead surface.** Neither is
referenced anywhere in `dblog.py`. `read_pk_range` exists solely to slice a key-width
chunk plan that no longer exists, and the Protocol still describes it that way
(`base.py:67-68`). They carry 39 integration tests' worth of maintenance. Keep them
deliberately (a future parallel dump wants `read_pk_range`) or drop them, but the
Protocol should not describe a plan the algorithm does not make.

**F9 — `build_connector`'s `*args` cannot be used.** `base.py:99` declares it,
`base.py:117` forwards it *after* the keyword arguments. Verified:
`build_connector('mssql','localhost','1433','sa','p','db','EXTRA')` →
`TypeError: MSSQLConnector.__init__() got multiple values for argument 'host'`.
Any caller passing a positional extra gets a confusing error about `host`.

**F10 — Bare `Exception` with an empty f-string.** `base.py:121`:
`raise Exception(f"Source of type not defined")` — no placeholder despite the `f`, and
the message does not name the type that was asked for. Verified: `SourceType.POSTGRES`
raises exactly that string. A `ValueError` naming the source type would be catchable
and diagnosable.

**F11 — ODBC connection-string injection.** `mssql.py:507-511` interpolates user,
password and database into a `;`-delimited connection string with no escaping. A
password containing `;` rewrites the connection — silently changing `Encrypt` or
`TrustServerCertificate`, for instance. ODBC's `{}` quoting rules are the fix.

**F12 — `application_name` is accepted, defaulted, logged, and never sent.**
`mssql.py:31` reads it, `mssql.py:513` logs it, and `conn_str` (`mssql.py:507-511`)
has no `APP=`. SQL Server's `program_name` never sees it, so the one operational use —
finding these sessions in `sys.dm_exec_sessions` — does not work.

**F13 — `assert` used for an invariant in shipped code.** `dblog.py:405`:
`assert self._spec is not None` inside `_key_after`. Stripped entirely under
`python -O`, while every other guard in the module raises `RuntimeError`.

**F14 — `DumpState` does not validate LSN width.** `state.py:43`. A truncated or
hand-edited state file loads happily and the failure surfaces much later, inside
`increment_lsn`, as "LSN must be 10 bytes". Validating on load points at the file.

**F15 — No way to reset a dump through `DBLog`.** `StateStore.clear` exists
(`state.py:75`) but `DBLog` exposes nothing, so a caller who wants to restart a
backfill has to reach into the store — which `DBLog` constructed itself when
`state_store` was left to default.

**F16 — Unused import.** `state.py:13`: `import json`. Everything goes through
pydantic's `model_dump_json` / `model_validate_json`.

**F17 — `os.replace` against the project's pathlib rule.** `state.py:138`.
`SKILL.md:35` says always use pathlib; `Path.replace()` is the direct equivalent and
equally atomic. Note the durability test monkeypatches `pydblog.state.os.replace`
(`test_state.py:162`), so the two move together.

---

## Low

**F18** — `inspect` has no docstring (`mssql.py:162`), the only public connector method
without one. `SKILL.md:32` requires them.

**F19** — `inspect` builds `business_columns` with a loop that unpacks four values and
discards three (`mssql.py:197-199`); the query selects `type_name`, `precision` and
`scale` and uses none of them. `SKILL.md:36` prefers a comprehension.

**F20** — `inspect`'s capture-instance query joins `sys.tables` and `sys.schemas`
without referencing either, and has no `ORDER BY` (`mssql.py:210-222`). A table with
two capture instances — the supported way to roll a schema change — returns an
arbitrary one.

**F21** — `capture_schema` is interpolated into that query unvalidated
(`mssql.py:165`, `:214`), while every other identifier goes through
`_validate_identifier`.

**F22** — `TableSpec` is mutable and mutated in place (`mssql.py:225`:
`spec.capture_instance = row[0]`). `SKILL.md:42` prefers immutable data; building the
spec once with the capture instance would do.

**F23** — `close()` leaves `_conn` set if the underlying close raises
(`mssql.py:515-519`), so a later call believes it is still connected.

**F24** — `read_event_log` materialises a whole window into one frame
(`mssql.py:257`). Window width is now bounded by a chunk scan rather than by a whole
dump, which is a real improvement, but it is still unbounded under sustained write
load.

**F25** — `read_pk_range`'s `isinstance(minimum, int)` (`mssql.py:478`) is True for a
`BIT` column's `True`/`False`. `DBLog._key_after` guards `bool` explicitly
(`dblog.py:419`); this one does not.

**F26** — `README.md` is empty (0 bytes), there is no `AGENTS.md`, and no `adls/`
folder. `SKILL.md:10-17` requires all three, and the algorithm's consumer contract
currently lives only in a docstring.

**F27** — `src/pydblog/__init__.py` is empty, so `from pydblog import DBLog` fails;
only `pydblog.connectors` re-exports anything.

**F28** — `[project.scripts]` is now an empty table (`pyproject.toml:22`), so the
package installs no entry point at all.

**F29** — A completed dump's drain loop never records its final position
(`dblog.py:292-294`): when the last window comes back empty-but-advancing, the loop
exits before `_checkpoint()`. Costs a re-read of an already-caught-up range on the next
run. Harmless under at-least-once, but it means `last_lsn` and the recorded state can
disagree.

**F30** — `run()`'s `if dump is not None and not self._dump_done:` (`dblog.py:265`)
duplicates the `while` condition on the next line; its actual job is choosing between
"dump then return" and "drain". It reads as a redundant guard.

**F31** — Two `pyrefly` warnings remain, both false: it narrows `_chunk_key` and
`_dump_done` from their assignments in `test_starting_a_run_clears_the_dump_state` and
does not model `drain()` mutating them, so it reads the later asserts as always-false.

**F32** — There is no CI configuration of any kind — no `.github/`, no
`.gitlab-ci.yml`. Every check in the "Verification" section below runs only when
somebody remembers to run it, on whatever interpreter they happen to have.

This now matters more than it did. `requires-python = ">=3.12"`
(`pyproject.toml:12`) claims an open-ended range, while `.python-version` pins 3.12 —
so 3.13 and 3.14 are entirely unexercised despite being declared supported. Testing
the floor is the right default (it catches newer syntax drifting in), but a range with
only one end tested is a claim rather than a fact. A two-entry matrix at 3.12 and the
current release would settle it.

**F33** — Nothing checks the health of the dependency set, and two defects sat in it
undetected until an unrelated change forced a re-resolve: `logfire[pydantic]` named an
extra that does not exist, so `uv` warned and silently ignored it on every single
resolve; and the `polars` pin had been *yanked* by its maintainers. Both are fixed, but
only by accident — the gap is that nothing would surface the next one. `uv lock
--check` in CI plus attention to resolver warnings would.

---

## Closed by the rewrite

Not to be re-raised. Previously: no handoff LSN (now `last_lsn`, `dblog.py:101`);
key-width chunking and its chunk-count blowup (now row-count keyset); no dump state
(now `state.py` plus checkpointing); a spurious retention error on a freshly enabled
capture instance (`_start` no longer calls `get_max_lsn()` at all); `_conn` narrowing
across seven call sites (now `_cursor()`, `mssql.py:75`); `build_connector` returning
`None` for `POSTGRES` (now raises, see F10); the broken `pydblog:main` console script
(entry removed, see F28); and the racy setup in `conftest` (now retried,
`conftest.py:252`, behind an Agent-readiness wait at `conftest.py:216`).

Since this audit was first written: the Python floor dropped from 3.14 to 3.12 —
verified on 3.12.13 across all 204 tests and `pyrefly`, and a fresh resolve at either
floor produces the same packages at the same versions, so the range costs nothing. The
nonexistent `logfire[pydantic]` extra and the yanked `polars` pin are both fixed; what
remains of them is F33.

---

## Decisions to settle

1. **F1** — emit a delete for the before-image key, refuse such tables, or document
   the limitation.
2. **F2** — reconcile against `cdc.captured_columns` at `inspect()`, or build the
   schema store the paper describes (§IV-F).
3. **F4** — what the first `DBLog` integration test should assert. The concurrent-write
   case is the one that actually tests the paper's claim, and it is also the fiddliest
   to make deterministic.
4. **F8** — keep `read_pk_range`/`end_pk` for a future parallel dump, or remove them.

## Verification

- `uv run pytest -m "not integration"` — 165 tests, no Docker.
- `uv run pytest` — adds the 39 connector integration tests; needs Docker.
- `pyrefly check` — expected clean, 2 known-false warnings (F31).
- `uv lock --check` — that the lock still matches `pyproject.toml`. Nothing runs this
  today (F33).
- The dev-compose lab (`docker compose -f dev-compose.yml up -d`) is what F1, F2 and
  F4 need to be reproduced against; `dbo.sales` already has the computed column that
  makes F2 observable.
