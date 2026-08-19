# The run is a value, not an object: one `dblog()` function that returns its state

- **Date:** 2026-08-19
- **Status:** Accepted
- **Scope:** `dblog.py`, `state.py`, `connectors/base.py`, and the consumer contract
- **Supersedes:** the storage half of `2026-08-17-1248-explicit-commit.md`, and
  `2026-08-17-1629-one-run-per-instance.md`

## Context

Two problems, and they turned out to be the same problem.

**A finished dump could not hand off.** Once the table was walked, the position a
log-only run needed to open at existed in `DBLog._last_lsn` and in a JSON file keyed by
dump name, and nowhere a caller could reach. Asking for it meant knowing the store, the
dump name, and that `StateStore.load` returns `None` for a dump that never ran. The
question that started this was literally "how do I get the final LSN so I can switch to
events only?", and the honest answer was that you could not.

**`commit()` was a hazard that had to be documented rather than removed.**
`2026-08-17-1248` made the write manual because a run recording its own position can
outrun the caller's durable write. That was the right call, but it left the correctness
of the whole thing resting on a paragraph of prose: call `commit()` after your write,
not before, and once more after the loop.

Both come from the position living somewhere the caller cannot see. So it stops living
there.

## Decision

One module-level function. No class.

```python
def dblog(
    connector: SourceConnector,
    schema: str,
    table: str,
    state: RunState | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    inspect_every: timedelta | None = DEFAULT_INSPECT_EVERY,
) -> BatchResult: ...
```

`BatchResult` is the frame and the state the call reached. The caller threads that state
into the next call and persists it wherever the data goes. Nothing in the package reads
or writes it.

The state controls everything, which is what removed the argument-combination rules
`_check_arguments` used to police:

| Input | What the call does |
| --- | --- |
| `state=None` | `inspect()`, open at `max(get_max_lsn(), floor)`, dump the table |
| a state | carry on from its `chunk_key` and `last_lsn` |
| a state with `dump_done=True` | a window of the log alone — the same call tails |

`commit()`, `StateStore`, `JsonFileStore` and `DumpState` are deleted. `DBLog` is
deleted; `SourceConnector` gained `__enter__`/`__exit__`, since the connection is now the
only thing with a lifetime to manage.

### Why this makes the commit hazard structural rather than documented

The frame and the position come back in the same object, at the same instant, before the
caller has written anything. There is no order of operations to get wrong: you either
persist both or neither, ideally in one transaction. A state that was never saved reads
the same batch again, which is at-least-once — the guarantee the window/supersede design
already makes.

`2026-08-17-1248`'s reasoning is unchanged and its conclusion is now enforced by the
shape instead of by a docstring.

### Why one function rather than `start()` + `fetch()`

Considered, and it reads well: no argument required only sometimes, and "start a new
dump" gets a name rather than being the absence of one. Rejected because the caller then
has to decide *which* to call, which means reproducing the `dump_done`/`None` logic at
every call site. `state=None` puts that decision in one place, and the cost — a `None`
that means "walk the whole table" — is paid for with a **warning-level** log line in
`_seed`. A caller whose state load quietly returned `None` re-dumps everything, and that
is expensive enough to say out loud.

### Why the class went entirely

Once the position is threaded, `DBLog` owns nothing. `schema`/`table` are arguments,
`chunk_size` is an argument, `dump`/`from_lsn` are subsumed by the state, `verbose` was a
call to the already-public `configure_logging`, `build_connector` was already a free
function, and `_spec`/`_last_lsn`/`_chunk_key`/`_dump_done` *are* the state. What was left
was the connection, and `SourceConnector` already is that object.

Keeping it as a thin wrapper was rejected specifically: it would hold the latest state as
a convenience, which reintroduces the exact ambiguity this removes — is the position in
the instance, or in the value I am holding? — and gives one algorithm two ways to be
driven.

This is also what resolves `2026-08-17-1629`. That ADR fixed the `dump_done` bug by
making run identity immutable per instance; the bug was that the flag lived on the
instance while the dump name arrived per call. With both in one threaded state there is
nothing left for them to disagree on, so per-call table naming is safe again — and
`schema`/`table` are now checked *against* the state rather than used, which is what
catches the wrong saved state being loaded.

### Why the `TableSpec` travels in the state

It is what settles the schema every frame shares, and it makes a pure function
affordable: `inspect()` runs once per dump rather than once per call. It also fixes
something that was quietly wrong before — a run resumed in a new process re-inspected,
so a table altered in between produced frames that no longer stacked with the ones
already written, and the consumer found out at its own `concat`. Carrying the spec pins
the run to the table as it was when the run opened.

The spec still has to be allowed to go stale on a run that lives for months, so
`RunState.last_inspect` records when it was read and `dblog()` re-reads it once a day by
default. What that re-read decides is only whether the *frame schema* moved:

- `captured_columns` or `pk_columns` differ → the capture instance changed under the run.
  `SchemaChangedError`, recovery `state=None`.
- `columns` alone differ → the source gained or lost a column CDC is not carrying.
  Neither read projects it, so no frame changes shape: **warning**, adopt, carry on.

Type drift comes for free: `inspect()` already calls `validate()`, which is where a
captured column that vanished from the source or whose Arrow type no longer matches is
caught.

`last_inspect` is stamped from `datetime.now(UTC)`, not `connector.watermark()`. The
watermark rule in `2026-08-14-1530` is about a mark compared against the source's own
record of progress; this one is compared only against itself, and a watermark would add a
round trip to every log-only call.

### Why retention is checked on every call

It used to be checked only when a run opened, which was right when a run was an object
that opened once. Now a state persisted days ago is the *ordinary* input, so a position
falling below the retention floor is something that happens while nobody is looking.
`dblog()` calls `get_min_lsn()` every call — one round trip alongside the `get_max_lsn()`
already there — so `CdcRetentionExpiredError` is a reliable signal rather than a
start-only surprise. That is what makes the external re-dump decision work:

```python
except (CdcRetentionExpiredError, SchemaChangedError):
    state = None
```

### Why `BatchResult` is an object and not a tuple

A caller who drops the second element of a tuple re-reads the same batch forever. And the
state must be kept even off the result whose frame is `None`: `_read_window` advances
`last_lsn` past a window that held nothing, deliberately, so an idle table does not
re-scan a widening range. A named field makes that visible; `_, state = dblog(...)` does
not. It is a frozen dataclass rather than a model because it holds a polars frame, which
nothing serializing the state should ever try to carry.

## Consequences

- **Breaking, and loudly so.** `DBLog` no longer exists, so every call site is an
  `ImportError` rather than a silent change of meaning. `commit()` is gone; there is
  nothing to call and nowhere it wrote to.
- **A capability was removed, not moved.** A run reading the log alone from a
  caller-chosen LSN — `dump=None, from_lsn=X` — has no equivalent. Every stream now
  begins with a dump, and a tail is what a finished dump's state becomes. The old
  `from_lsn` was also the only way to replay log history from before the present, and
  that goes with it. Restoring either means hand-constructing a `RunState`, which needs
  a spec, which needs an `inspect()` — so if a caller ever wants it, the right shape is a
  small public helper, not a resurrected argument.
- **The default state file is gone.** `JsonFileStore` wrote to `.pydblog-state` in the
  working directory by default, which was a landmine in any process whose cwd was not
  what its author assumed. Persistence is now visibly the caller's, and
  `test_a_run_writes_nothing_of_its_own` pins that nothing reappears.
- `tests/test_dblog.py` lost its `factory_calls` fixture and every test about
  construction, `verbose` and the `DBLog` context manager: those were about an object
  that no longer exists, and what remains of them belongs to the connector.
  `test_state.py` lost the whole store — file round trip, name escaping, the
  `os.replace` crash simulation — and is now about the model.
- The integration fixtures got simpler rather than harder, which is the useful signal:
  the module used to need a `MemoryStore` and a `DBLog` factory to give two runs their
  own identity. Now they share one connector and keep their own state, and the handoff
  the whole change exists for is directly testable —
  `test_the_tail_carries_what_was_written_after_the_dump` walks the table, keeps the
  final state, writes a row, and reads it back through that state alone.

## What was measured

- The full non-integration suite: 278 passing, 0.35s.
- `tests/test_dblog_integration.py` against a real SQL Server via testcontainers: 15
  passing in 84s, including
  `test_a_write_during_a_chunk_scan_does_not_escape_as_a_stale_row` — the barrier
  property from `2026-08-14-1530` — which is unchanged by this rewrite, as it should be.
  The watermark ordering inside `dblog()` is byte-for-byte the ordering it had inside
  `DBLog.fetch()`.
- `uv run pyrefly check src`: 0 errors.
