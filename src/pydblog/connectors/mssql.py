import logging
import re
from datetime import datetime

import mssql_python
from polars import DataFrame

from pydblog.connectors.base import LSN, TableSpec

# CDC operation codes, from the __$operation column.
OP_DELETE = 1
OP_INSERT = 2
OP_UPDATE_BEFORE = 3
OP_UPDATE_AFTER = 4

# An LSN is a fixed-width big-endian binary(10), which is why byte order is numeric
# order and comparing LSNs as plain bytes works.
LSN_WIDTH = 10

logger = logging.getLogger(__name__)


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

    def inspect(self,
        schema: str,
        table: str,
        capture_schema: str = "cdc"
    ) -> TableSpec:

        cur = self._cursor()

        object_name = f"{schema}.{table}"

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

        cur.execute(
            "SELECT c.name, t.name AS type_name, c.precision, c.scale "
            "FROM sys.columns c "
            "JOIN sys.types t ON t.user_type_id = c.user_type_id "
            "WHERE c.object_id = OBJECT_ID(?) AND c.is_computed = 0 "
            "ORDER BY c.column_id",
            [object_name],
        )

        business_columns: list[str] = []
        for name, type_name, precision, scale in cur.fetchall():
            business_columns.append(name)



        spec = TableSpec(
            source_schema=schema,
            source_table=table,
            pk_columns=pk_columns,
            business_columns=business_columns,
        )

        cur.execute(
            f"""
            SELECT
                ct.capture_instance
            FROM {capture_schema}.change_tables ct
            JOIN sys.tables t
                ON ct.source_object_id = t.object_id
            JOIN sys.schemas s
                ON t.schema_id = s.schema_id
            WHERE source_object_id = OBJECT_ID(?)
            """,
            [object_name],
        )
        row = cur.fetchone()
        if row:
            spec.capture_instance = row[0]
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
        ]
        columns += [self._quote_identifier(col, "business column") for col in spec.business_columns]

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



        return DataFrame(arrow_table)


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
        columns = [self._quote_identifier(col, "business column") for col in spec.business_columns]
        order_by = [self._quote_identifier(col, "primary key column") for col in spec.pk_columns]
        leading = order_by[0]

        clauses = [f"SELECT {', '.join(columns)}", f"FROM {schema}.{table}"]
        params: list[int] = []

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
        # uncapped read drops the whole pair rather than passing zero.
        if limit > 0:
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
        # a row count. Arrow keeps the result set schema even at zero rows.
        return DataFrame(arrow_table)

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
