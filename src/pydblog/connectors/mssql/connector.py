import logging
import re
from datetime import datetime
from math import ceil
from time import monotonic, sleep

import mssql_python
from polars import Binary, DataFrame, Datetime, Int32, all, lit

from pydblog.connectors.base import LSN, TableSpec
from pydblog.connectors.mssql.schema import conform, event_schema, row_schema, validate
from pydblog.connectors.types import ColumnSpec

# CDC operation codes, from the __$operation column. Zero is not one of them, which
# is what makes it available to mark a row that came off the table rather than the log.
OP_DUMP = 0
OP_DELETE = 1
OP_INSERT = 2
OP_UPDATE_BEFORE = 3
OP_UPDATE_AFTER = 4

# An LSN is a fixed-width big-endian binary(10), which is why byte order is numeric
# order and comparing LSNs as plain bytes works.
LSN_WIDTH = 10

WATERMARK_TIMEOUT_SECONDS = 120.0
WATERMARK_POLL_SECONDS = 1.0

logger = logging.getLogger(__name__)


def _mask_width(spec: TableSpec) -> int:
    """Bytes in a change table's update mask: one bit per captured column."""
    return ceil(len(spec.business_columns) / 8)


class MSSQLConnector:

    def __init__(self, host: str, user: str, password: str, database: str, port: str, *args, **kwargs):
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._port = port
        self._encrypt: str = kwargs.get("encrypt", "yes")
        self._trust_server_certificate: str = kwargs.get("trust_server_certificate", "yes")
        self._application_name: str = kwargs.get("application_name", "Lakehouse DBLog")
        self._watermark_timeout = float(
            kwargs.get("watermark_timeout", WATERMARK_TIMEOUT_SECONDS)
        )
        self._watermark_poll = float(
            kwargs.get("watermark_poll", WATERMARK_POLL_SECONDS)
        )
        self._pagination = kwargs.get("pagination", "fetch")
        if self._pagination not in ("fetch", "top"):
            raise ValueError(
                f"pagination must be 'fetch' or 'top', got {self._pagination!r}"
            )

        # Identifiers (schema, table, column, capture instance) go into the query by
        # interpolation — the driver cannot bind an object name. This regex is the barrier.
        self._identifier_pattern = re.compile(r"[A-Za-z0-9_]+")

        self._conn: mssql_python.Connection | None = None

    def _validate_identifier(self, name: str, kind: str = "identifier") -> str:
        """Check that a name can be interpolated into SQL without injection risk.

        Brackets (``[name]``) are not enough on their own: a ``]`` inside the name
        closes the delimiter and whatever follows becomes SQL. Validation is what
        actually holds the line.

        Args:
            name: The identifier to validate.
            kind: How the identifier is described in the error message.

        Returns:
            The ``name`` itself, unchanged.

        Raises:
            ValueError: If ``name`` holds a character outside ``[A-Za-z0-9_]``, or is empty.
        """
        if not self._identifier_pattern.fullmatch(name):
            raise ValueError(f"Invalid {kind} name: {name!r}")
        return name

    def _quote_identifier(self, name: str, kind: str = "identifier") -> str:
        """Validate an identifier and wrap it in T-SQL brackets.

        Args:
            name: The identifier to delimit.
            kind: How the identifier is described in the error message.

        Returns:
            The bracketed identifier, for example ``[sale_id]``.

        Raises:
            ValueError: If ``name`` is not a safe identifier.
        """
        return f"[{self._validate_identifier(name, kind)}]"

    def _cursor(self) -> mssql_python.Cursor:
        """Open a cursor, connecting first if there is no connection yet.

        Every read starts this way, so it lives here rather than being repeated. It
        is also what lets ``_conn`` stay ``Connection | None``: the guard below turns
        a type the checker cannot narrow across a call boundary into one it can.

        Returns:
            A cursor on the open connection.

        Raises:
            RuntimeError: If the connection is missing after ``connect()`` returned,
                which it never is.
        """
        self.connect()

        if self._conn is None:
            raise RuntimeError("connect() returned without opening a connection")

        return self._conn.cursor()

    def get_min_lsn(self, capture_instance: str) -> LSN:
        """Fetch the oldest LSN CDC still retains for a capture instance.

        This is the retention floor, and it *advances* as the cleanup job runs — a
        read position that falls below it points at events that no longer exist.

        Args:
            capture_instance: The CDC capture instance name.

        Returns:
            The lowest available LSN.

        Raises:
            ValueError: If CDC does not recognise the capture instance, or the caller is
                not authorized to read its change data.
        """
        cur = self._cursor()
        cur.execute("SELECT sys.fn_cdc_get_min_lsn(?) as min_lsn", [capture_instance])
        row = cur.fetchone()
        cur.close()

        # Documented behaviour: fn_cdc_get_min_lsn returns 0x0000000000000000_0000 "when
        # the capture instance does not exist or when the caller is not authorized to
        # access the change data associated with the capture instance". It must be
        # rejected, because every LSN compares >= zero — so a retention check against a
        # zero floor silently passes, and the snapshot then reads no events at all while
        # reporting success. That is precisely the failure the check exists to catch, and
        # the unauthorized case makes it a live risk for any least-privilege account, not
        # just a typo in a capture instance name. A real one always has a non-zero floor,
        # being the start_lsn recorded when CDC was enabled on the table.
        if row is None or row[0] is None or row[0] == bytes(LSN_WIDTH):
            raise ValueError(
                f"CDC returned no minimum LSN for capture instance {capture_instance!r}; "
                f"is CDC enabled on that table, and does this login have SELECT on its "
                f"captured columns?"
            )

        return row[0]

    def get_max_lsn(self) -> LSN:
        """Fetch the highest LSN the CDC capture job has processed.

        This lags live commits — the capture job scans the log periodically. That
        lag is safe for the chunk loop: it can only push an event into a later
        window, never an earlier one.

        Returns:
            The database-wide maximum LSN.

        Raises:
            ValueError: If CDC has not produced a watermark yet, which it signals by
                returning NULL.
        """
        cur = self._cursor()
        cur.execute("SELECT sys.fn_cdc_get_max_lsn() as max_lsn")
        row = cur.fetchone()
        cur.close()

        if row is None or row[0] is None:
            raise ValueError(
                "CDC returned no maximum LSN; is CDC enabled on this database and the "
                "SQL Server Agent running?"
            )

        return row[0]

    def _read_pk_columns(self, cur: mssql_python.Cursor, object_name: str) -> list[str]:
        """Read a table's primary key columns in key order.

        Raises:
            ValueError: If the table has no primary key, which is also how an unknown
                table surfaces: OBJECT_ID returns NULL and the index join finds nothing.
        """
        cur.execute(
            "SELECT c.name FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id "
            "AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id "
            "AND c.column_id = ic.column_id "
            "WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(?) "
            "ORDER BY ic.key_ordinal",
            [object_name],
        )

        pk_columns = [row[0] for row in cur.fetchall()]

        if not pk_columns:
            raise ValueError(f"{object_name} does not have primary key")

        return pk_columns

    def _read_columns(
        self, cur: mssql_python.Cursor, object_name: str
    ) -> list[ColumnSpec]:
        """Read an object's columns, with the type metadata, in ordinal order.

        Serves both the source table and the change table: the latter is an ordinary
        table too, which is why its captured types can be read the same way rather
        than out of ``cdc.captured_columns`` — that view records the type name but
        neither the precision nor the scale, and a decimal needs both.
        """
        cur.execute(
            "SELECT c.name, t.name AS type_name, c.precision, c.scale, cc.definition "
            "FROM sys.columns c "
            "JOIN sys.types t ON t.user_type_id = c.user_type_id "
            "LEFT JOIN sys.computed_columns cc "
            "ON cc.object_id = c.object_id AND cc.column_id = c.column_id "
            "WHERE c.object_id = OBJECT_ID(?) "
            "ORDER BY c.column_id",
            [object_name],
        )

        return [
            ColumnSpec(
                name=name,
                type_name=type_name,
                precision=precision,
                scale=scale,
                computed_definition=definition,
            )
            for name, type_name, precision, scale, definition in cur.fetchall()
        ]

    def _read_capture_instance(
        self, cur: mssql_python.Cursor, object_name: str, capture_schema: str
    ) -> str | None:
        """Read the capture instance CDC keeps for a table, or None if there is none."""
        cur.execute(
            f"SELECT ct.capture_instance "
            f"FROM {capture_schema}.change_tables ct "
            f"WHERE ct.source_object_id = OBJECT_ID(?)",
            [object_name],
        )
        row = cur.fetchone()

        return row[0] if row else None

    def _read_captured_columns(
        self, cur: mssql_python.Cursor, capture_instance: str, capture_schema: str
    ) -> list[ColumnSpec]:
        """Read the business columns of a capture instance's change table.

        The change table holds the log's own columns ahead of the table's, all named
        with a ``__$`` prefix the source cannot use, so dropping that prefix leaves
        exactly the captured columns — in the order the change table stores them,
        which is the order every event read returns.
        """
        change_table = (
            f"{capture_schema}."
            f"{self._validate_identifier(capture_instance, 'capture instance')}_CT"
        )

        return [
            column
            for column in self._read_columns(cur, change_table)
            if not column.name.startswith("__$")
        ]

    def _mark_computed(
        self, captured: list[ColumnSpec], source: list[ColumnSpec]
    ) -> list[ColumnSpec]:
        """Carry each computed column's formula over from the source table.

        The change table holds a computed column as a plain one and never puts a value
        in it, so the formula only exists on the source side. Copying it here is what
        lets both reads null the column deliberately, and what leaves a consumer the
        expression to recompute from.
        """
        formulas = {
            column.name: column.computed_definition
            for column in source
            if column.is_computed
        }

        if formulas:
            logger.info(f"computed columns read as null: {sorted(formulas)}")

        return [
            column.model_copy(update={"computed_definition": formulas[column.name]})
            if column.name in formulas
            else column
            for column in captured
        ]

    def inspect(self, schema: str, table: str, capture_schema: str = "cdc") -> TableSpec:
        """Read the metadata every read of a table is driven from.

        The two column lists are read separately and reconciled here, because they are
        what the two read paths project and they are free to disagree: CDC records a
        column's type when capture is enabled and keeps it, so a later ``ALTER COLUMN``
        leaves the change table returning one type and the source table another.
        Catching that here costs a query; catching it partway through a dump costs the
        chunk that was in flight when the cast failed.

        Args:
            schema: Schema of the table to inspect.
            table: Name of the table to inspect.
            capture_schema: Schema CDC keeps its own objects in.

        Returns:
            The table's metadata, with a schema both read paths can conform to.

        Raises:
            ValueError: If the table has no primary key, if the two column lists
                cannot produce one schema, or if a column has no Arrow equivalent.
        """
        cur = self._cursor()
        object_name = f"{schema}.{table}"

        pk_columns = self._read_pk_columns(cur, object_name)
        # Ahead of the columns, because the change table's name is built from it.
        capture_instance = self._read_capture_instance(cur, object_name, capture_schema)
        columns = self._read_columns(cur, object_name)
        captured_columns = (
            # With no change log there is nothing to reconcile against, and the
            # table's own columns are what a read projects.
            columns
            if capture_instance is None
            else self._mark_computed(
                self._read_captured_columns(cur, capture_instance, capture_schema),
                columns,
            )
        )

        spec = TableSpec(
            source_schema=schema,
            source_table=table,
            pk_columns=pk_columns,
            columns=columns,
            captured_columns=captured_columns,
            capture_instance=capture_instance,
        )
        cur.close()

        # Nothing to reconcile without a change table, and a table that has no capture
        # instance already fails with a clearer message when a run starts on it.
        if capture_instance is not None:
            validate(spec)

        return spec


    def read_event_log(self, spec: TableSpec, from_lsn: LSN, to_lsn: LSN) -> DataFrame | None:
        if spec.capture_instance is None:
            raise ValueError(
                f"{spec.qualified_name} does not have a CDC capture instance; call inspect() first"
            )

        # The capture instance is part of the function name and cannot be a parameter,
        # so it has to be validated as a safe SQL identifier.
        self._validate_identifier(spec.capture_instance, "capture instance")

        columns = [
            "__$start_lsn AS start_lsn",
            "__$seqval AS seqval",
            "__$operation AS operation",
            "__$update_mask AS update_mask",
            # Read here rather than mapped per LSN afterwards: the commit time is a
            # property of the event, and one round trip carries the whole window.
            "sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS commit_timestamp",
        ]
        # Every captured column, computed ones included — CDC records them as null,
        # which is what a dump row carries for them too.
        columns += [
            self._quote_identifier(col, "business column") for col in spec.business_columns
        ]

        query = (
            f"SELECT {', '.join(columns)} "
            f"FROM cdc.fn_cdc_get_all_changes_{spec.capture_instance}(?, ?, N'all') "
            "ORDER BY __$start_lsn, __$seqval"
        )

        logger.debug(f"generated query:\n{query}")

        cur = self._cursor()
        cur.execute(query, [from_lsn, to_lsn])
        arrow_table = cur.arrow()
        cur.close()

        logger.info(
            f"read {arrow_table.num_rows} events from {spec.capture_instance} "
            f"in ({from_lsn.hex()}, {to_lsn.hex()}]"
        )

        if arrow_table.num_rows == 0:
            return None

        return DataFrame(
            conform(arrow_table, event_schema(spec), f"{spec.capture_instance} events")
        )

    def to_events(
        self, rows: DataFrame, spec: TableSpec, commit_timestamp: datetime | None
    ) -> DataFrame:
        """Stamp a chunk of table rows with the log's own columns.

        What makes a dump run yield one schema instead of two: a chunk arrives with
        the table's columns only, and comes out of here shaped like an event.

        The LSN columns are all zeros, which is a position CDC never issues. It marks
        the row as read off the table rather than out of the log — legible to anyone
        auditing the output — and it gives the right precedence for nothing: ordering
        by ``(start_lsn, seqval)`` puts every dump row below every real event, which
        is the base-image-then-changes relationship the merge already enforces.

        Args:
            rows: A chunk of table rows, conforming to the spec's row schema.
            spec: Table metadata.
            commit_timestamp: Commit time of the window bracketing the chunk read, or
                None when the log records none for it.

        Returns:
            The same rows, in the event schema.
        """
        stamped = rows.select(
            lit(bytes(LSN_WIDTH), Binary).alias("start_lsn"),
            lit(bytes(LSN_WIDTH), Binary).alias("seqval"),
            lit(OP_DUMP, Int32).alias("operation"),
            lit(bytes(_mask_width(spec)), Binary).alias("update_mask"),
            lit(commit_timestamp, Datetime("us")).alias("commit_timestamp"),
            all(),
        )

        logger.debug(f"stamped {stamped.height} dump rows as events")

        return stamped

    def watermark(self) -> datetime:
        """Take a watermark from the source's own clock.

        Read from the source rather than the client: it is compared against times
        SQL Server recorded for its own capture scans.

        Returns:
            The source's current time.
        """
        cur = self._cursor()
        cur.execute("SELECT SYSDATETIME()")
        row = cur.fetchone()
        cur.close()

        if row is None or row[0] is None:
            raise RuntimeError("the source returned no time for a watermark")

        return row[0]

    def await_watermark(self, mark: datetime) -> None:
        """Block until the capture job has processed everything committed by ``mark``.

        ``fn_cdc_get_max_lsn()`` reports how far the capture job has read, not how far
        the database has committed, so a window bounded by it can miss a change made
        during the chunk scan. This blocks until ``_capture_passed`` confirms the
        capture job has caught up past ``mark``.

        Costs one capture polling interval (5s by default); ``watermark_timeout`` and
        ``watermark_poll`` on the constructor tune it for a different polling rate.

        Args:
            mark: A watermark from ``watermark()``.

        Raises:
            TimeoutError: If the capture job did not pass ``mark`` in time — normally
                meaning it is not running.
        """
        started = monotonic()
        deadline = started + self._watermark_timeout
        cur = self._cursor()

        logger.debug(f"waiting for capture to pass watermark {mark.isoformat()}")

        try:
            while monotonic() < deadline:
                if self._capture_passed(cur, mark):
                    logger.info(
                        f"capture passed watermark {mark.isoformat()} after "
                        f"{monotonic() - started:.1f}s"
                    )
                    return

                sleep(self._watermark_poll)
        finally:
            cur.close()

        logger.warning(
            f"capture did not pass watermark {mark.isoformat()} within "
            f"{self._watermark_timeout}s"
        )
        raise TimeoutError(
            f"the capture job did not pass the watermark {mark.isoformat()} within "
            f"{self._watermark_timeout}s; is the SQL Server Agent running and the "
            f"capture job started?"
        )

    def _capture_passed(self, cur: mssql_python.Cursor, mark: datetime) -> bool:
        """Whether the capture job has processed everything committed by ``mark``.

        Either a scan started after ``mark`` (writing) or an empty one ended after it
        (idle and caught up) — the second case matters because consecutive empty
        scans reuse one session row, freezing ``start_time``, which is a dump's usual
        state since it only reads.
        """
        cur.execute(
            "SELECT TOP 1 CASE WHEN start_time > ? "
            "    OR (empty_scan_count > 0 AND end_time > ?) THEN 1 ELSE 0 END "
            "FROM sys.dm_cdc_log_scan_sessions "
            "WHERE session_id > 0 "
            "ORDER BY end_time DESC",
            [mark, mark],
        )
        row = cur.fetchone()

        return bool(row and row[0])

    def map_lsn_to_timestamp(self, lsn: LSN) -> datetime | None:
        cur = self._cursor()
        cur.execute("SELECT sys.fn_cdc_map_lsn_to_time(?)", [lsn])
        row = cur.fetchone()
        cur.close()

        if row:
            return row[0]
        return None

    def _validate_read_spec(self, spec: TableSpec) -> None:
        """Check a spec can be read from at all.

        Raises:
            ValueError: If the spec has no primary key or no business columns.
        """
        if not spec.pk_columns:
            raise ValueError(
                f"{spec.qualified_name} has no primary key columns; "
                f"read_table needs them for a deterministic ORDER BY"
            )

        if not spec.business_columns:
            raise ValueError(f"{spec.qualified_name} has no business columns to read")

    def _project(self, column: ColumnSpec) -> str:
        """How a column is named in a table read's SELECT list.

        A computed column is projected as NULL rather than read. SQL Server computes
        it on the way out of the table but records nothing for it in the change log,
        so reading its value here would put the computed number on a dump row and a
        null on every event for that same row — one schema carrying two answers, and
        no way downstream to tell that from a real transition to null. Null on both
        sides is the honest one, and ``computed_definition`` on the spec is what a
        consumer recomputes from.
        """
        quoted = self._quote_identifier(column.name, "business column")

        return f"NULL AS {quoted}" if column.is_computed else quoted

    def _build_read_table_query(
        self,
        spec: TableSpec,
        start_pk: int | None,
        end_pk: int | None,
        limit: int,
    ) -> tuple[str, list[int]]:
        """Build the T-SQL for a keyset range read, with its parameters.

        The range predicate is on the *leading* primary key column only, which is
        what makes chunks stable: the bounds are key values, so concurrent DML cannot
        move a row from one chunk to another. Against a composite key this still
        partitions the table completely — every row has exactly one leading-column
        value — it just makes rows-per-chunk vary more.

        The ORDER BY uses the *whole* primary key, which is unique. Ordering by a
        prefix would leave ties up to the execution plan, and the row order within a
        chunk would not be reproducible.

        Query and parameters are built together so a bound and the value it binds can
        never drift into a placeholder count that does not match.

        Args:
            spec: Table metadata.
            start_pk: Inclusive lower bound on the leading key; ``None`` for unbounded.
            end_pk: Exclusive upper bound on the leading key; ``None`` for unbounded.
            limit: Optional cap on rows read; ``0`` means uncapped.

        Returns:
            The query with its ``?`` placeholders, and the values to bind to them.

        Raises:
            ValueError: If the spec has no primary key or no business columns, if
                ``limit`` is negative, if the bounds are inverted, or if any
                identifier is unsafe.
        """
        self._validate_read_spec(spec)

        if limit < 0:
            raise ValueError(f"limit must be >= 0, got {limit}")

        if start_pk is not None and end_pk is not None and start_pk >= end_pk:
            raise ValueError(
                f"start_pk must be < end_pk for a non-empty range, got {start_pk} >= {end_pk}"
            )

        schema = self._quote_identifier(spec.source_schema, "schema")
        table = self._quote_identifier(spec.source_table, "table")
        columns = [self._project(column) for column in spec.captured_columns]
        order_by = [self._quote_identifier(col, "primary key column") for col in spec.pk_columns]
        leading = order_by[0]

        select = f"SELECT TOP (?) {', '.join(columns)}" if (
            limit > 0 and self._pagination == "top"
        ) else f"SELECT {', '.join(columns)}"
        clauses = [select, f"FROM {schema}.{table}"]
        params: list[int] = []

        if limit > 0 and self._pagination == "top":
            params.append(limit)

        # Both predicates are plain comparisons on the leading key column, so the
        # optimizer gets a clean seek on the clustered primary key index.
        predicates = []
        if start_pk is not None:
            predicates.append(f"{leading} >= ?")
            params.append(start_pk)
        if end_pk is not None:
            predicates.append(f"{leading} < ?")
            params.append(end_pk)
        if predicates:
            clauses.append(f"WHERE {' AND '.join(predicates)}")

        clauses.append(f"ORDER BY {', '.join(order_by)}")

        # T-SQL requires OFFSET before FETCH, and rejects FETCH NEXT 0 ROWS ONLY, so an
        # uncapped read drops the clause rather than passing zero. TOP takes the same
        # cap earlier, right after SELECT, since T-SQL requires it there.
        if limit > 0 and self._pagination == "fetch":
            clauses.append("OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY")
            params.append(limit)

        return " ".join(clauses), params

    def read_table(
        self,
        spec: TableSpec,
        start_pk: int | None = None,
        end_pk: int | None = None,
        limit: int = 0,
    ) -> DataFrame:
        """Read a half-open key range ``[start_pk, end_pk)`` from the table.

        Pagination is by key value, not by position. That is what lets a chunk plan be
        computed once up front and stay valid: an OFFSET-based read would have every
        later page shift when a row below it is deleted, and the row that slid across
        the boundary would be read by no chunk at all — with no CDC event to repair
        it, since it was never itself modified.

        Args:
            spec: Table metadata, normally coming from ``inspect()``.
            start_pk: Inclusive lower bound on ``spec.pk_columns[0]``; ``None`` reads
                from the start of the table.
            end_pk: Exclusive upper bound on ``spec.pk_columns[0]``; ``None`` reads to
                the end.
            limit: Optional cap on rows read; ``0`` means uncapped.

        Returns:
            A DataFrame holding the ``spec.business_columns``, in that order, sorted
            by the full primary key.

        Raises:
            ValueError: If ``limit`` is negative, if the bounds are inverted, if the
                spec has no primary key or no business columns, or if any identifier
                is unsafe.
        """
        query, params = self._build_read_table_query(spec, start_pk, end_pk, limit)

        logger.debug(f"generated query:\n{query}")

        cur = self._cursor()
        cur.execute(query, params)
        arrow_table = cur.arrow()
        cur.close()

        logger.info(
            f"read {arrow_table.num_rows} rows from {spec.qualified_name} "
            f"[{start_pk}, {end_pk}) limit={limit}"
        )

        # Unlike read_event_log, an empty range returns an empty DataFrame rather than
        # None: a key range with no rows in it is a legitimate answer, not an absent
        # result — sparse ranges are expected, since chunk_size is a key width and not
        # a row count. The declared schema survives zero rows either way.
        return DataFrame(
            conform(arrow_table, row_schema(spec), f"{spec.qualified_name} rows")
        )

    def _build_pk_range_query(self, spec: TableSpec) -> str:
        """Build the T-SQL that reads the leading key's bounds.

        Raises:
            ValueError: If the spec has no primary key, or an identifier is unsafe.
        """
        self._validate_read_spec(spec)

        schema = self._quote_identifier(spec.source_schema, "schema")
        table = self._quote_identifier(spec.source_table, "table")
        leading = self._quote_identifier(spec.pk_columns[0], "primary key column")

        return f"SELECT MIN({leading}), MAX({leading}) FROM {schema}.{table}"

    def read_pk_range(self, spec: TableSpec) -> tuple[int, int] | None:
        """Read the lowest and highest value of the leading primary key column.

        Two index seeks against the clustered primary key — this is what the chunk
        plan is sliced out of.

        Args:
            spec: Table metadata.

        Returns:
            ``(min, max)``, or ``None`` when the table is empty.

        Raises:
            ValueError: If the spec has no primary key, if an identifier is unsafe, or
                if the leading key column is not an integer type — value arithmetic
                cannot slice a key space it cannot count.
        """
        query = self._build_pk_range_query(spec)

        logger.debug(f"generated query:\n{query}")

        cur = self._cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()

        if row is None or row[0] is None:
            logger.info(f"read primary key range of {spec.qualified_name}: empty")
            return None

        minimum, maximum = row[0], row[1]
        if not isinstance(minimum, int) or not isinstance(maximum, int):
            raise ValueError(
                f"{spec.qualified_name} has a non-integer leading primary key column "
                f"{spec.pk_columns[0]!r} (got {type(minimum).__name__}); range chunking "
                f"needs an integer key"
            )

        logger.info(
            f"read primary key range of {spec.qualified_name}: [{minimum}, {maximum}]"
        )

        return minimum, maximum

    def connect(self) -> None:
        """Open the connection, if there is not one already.

        Idempotent and cheap, which is what lets every read method start by calling
        it instead of requiring the caller to connect first.

        There is deliberately no liveness check here. Physical connection health is
        the ``mssql_python`` pool's job, and it is enabled by default; the way this
        connector uses the connection is stateless — ``autocommit=True``, every method
        is a self-contained query, with no open transaction, temp table or any session
        state to reconcile. An application-side probe would only spend an extra
        round-trip re-deciding what the pool already decided.
        """
        if self._conn is not None:
            return

        conn_str =  (
            f"SERVER={self._host},{self._port};DATABASE={self._database};"
            f"Uid={self._user};Pwd={self._password};"
            f"Encrypt={self._encrypt};TrustServerCertificate={self._trust_server_certificate};"
        )
        self._conn = mssql_python.connect(conn_str, autocommit=True)
        logger.info(
            f"connected to {self._host}:{self._port}/{self._database} "
            f"as {self._user} ({self._application_name})"
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            logger.info(f"disconnected from {self._host}:{self._port}/{self._database}")
            self._conn = None

    def increment_lsn(self, lsn: LSN) -> LSN:
        """Return the next LSN after ``lsn``.

        Equivalent to ``sys.fn_cdc_increment_lsn``, without the round trip.

        This exists because ``fn_cdc_get_all_changes`` is inclusive on *both* bounds.
        Reading consecutive windows as ``[last, high]`` then ``[high, high']`` would
        re-emit whatever event sits exactly at ``high``, once per chunk. Starting each
        window one LSN later is what makes them half-open.

        Args:
            lsn: A 10-byte LSN.

        Returns:
            The immediately following LSN, same width.

        Raises:
            ValueError: If ``lsn`` is the wrong width, or is the maximum LSN — wrapping
                to zero would silently rewind the read position.
        """
        if len(lsn) != LSN_WIDTH:
            raise ValueError(f"LSN must be {LSN_WIDTH} bytes, got {len(lsn)}: {lsn!r}")

        try:
            return (int.from_bytes(lsn, "big") + 1).to_bytes(LSN_WIDTH, "big")
        except OverflowError as exc:
            raise ValueError(f"cannot increment the maximum LSN {lsn.hex()}") from exc
