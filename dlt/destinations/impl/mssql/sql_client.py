from dlt.common.destination import DestinationCapabilitiesContext

import mssql_python

from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, Tuple

from dlt.destinations.exceptions import (
    DatabaseException,
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.typing import DBApi, DBTransaction
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials
from dlt.common.destination.dataset import DBApiCursor


class MsSqlClient(SqlClientBase[mssql_python.Connection], DBTransaction):
    dbapi: ClassVar[DBApi] = mssql_python

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MsSqlCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: mssql_python.Connection = None
        self.credentials = credentials

    def open_connection(self) -> mssql_python.Connection:
        self._conn = mssql_python.connect(
            self.credentials.to_odbc_dsn(),
            autocommit=True,
            timeout=self.credentials.connect_timeout,
        )
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.autocommit = False
            yield self
            self.commit_transaction()
        except BaseException:
            # A failed rollback must not replace the error that aborted the transaction.
            with suppress(DatabaseException):
                self.rollback_transaction()
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()
        self._conn.autocommit = True

    @raise_database_error
    def rollback_transaction(self) -> None:
        try:
            self._conn.rollback()
        except mssql_python.ProgrammingError as ex:
            # Synapse can invalidate the transaction when a statement fails.
            message = ex.ddbc_error.lower()
            if "(111214)" not in message and "no corresponding transaction found" not in message:
                raise
        finally:
            self._conn.autocommit = True

    @property
    def native_connection(self) -> mssql_python.Connection:
        return self._conn

    def drop_dataset(self) -> None:
        # MS Sql doesn't support DROP ... CASCADE, drop tables in the schema first
        # Drop all views
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        view_names = [row[0] for row in rows]
        self._drop_views(*view_names)
        # Drop all tables
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        table_names = [row[0] for row in rows]
        self.drop_tables(*table_names)
        # Drop schema
        self._drop_schema()

    def _drop_views(self, *tables: str) -> None:
        if not tables:
            return
        statements = [
            f"DROP VIEW IF EXISTS {self.make_qualified_table_name(table)}" for table in tables
        ]
        self.execute_many(statements)

    def _drop_schema(self) -> None:
        self.execute_sql("DROP SCHEMA %s" % self.fully_qualified_dataset_name())

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        if args and kwargs:
            raise TypeError("Cannot mix positional and named query parameters")
        if args:
            # dlt emits %s positional placeholders; mssql-python expects qmark (?)
            # TODO: this is bad. See duckdb & athena also
            query = query.replace("%s", "?")
        # Own the cursor lifetime so cleanup cannot mask a query failure.
        curr = self._conn.cursor()
        try:
            if kwargs:
                # mssql-python's paramstyle is pyformat: pass named parameters (%(name)s) through
                curr.execute(query, kwargs)
            else:
                # unpack because empty tuple gets interpreted as a single argument
                curr.execute(query, *args)
            # NOTE: firsts recordset is wrapped in a cursor
            yield DBApiCursorImpl(curr)  # type: ignore[arg-type]
            # Later statements in a batch may fail only when advancing to their results.
            while curr.nextset():
                pass
        except BaseException as ex:
            # close() discards pending results. Do not execute more of a failed batch or
            # let cleanup failures replace an execution, fetch or consumer exception.
            with suppress(mssql_python.Error):
                curr.close()
            if isinstance(ex, mssql_python.Error):
                with suppress(mssql_python.Error):
                    self._conn.rollback()
            raise
        else:
            curr.close()

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if not isinstance(ex, mssql_python.Error):
            return ex
        # mssql-python maps the SQLSTATE to a stable `driver_error` label, which we classify on
        # (the ddbc_error message is server/locale dependent, the label is not).
        driver_error = ex.driver_error
        if isinstance(ex, mssql_python.ProgrammingError):
            if driver_error == "Base table or view not found":  # SQLSTATE 42S02
                return DatabaseUndefinedRelation(ex)
            if driver_error == "Syntax error or access violation":  # SQLSTATE 42000
                # The driver may omit native error 15151, leaving only the server message.
                msg = ex.ddbc_error.lower()
                if (
                    "(15151)" in msg
                    or "because it does not exist or you do not have permission" in msg
                ):
                    return DatabaseUndefinedRelation(ex)
                return DatabaseTerminalException(ex)
            if driver_error == "COUNT field incorrect":  # SQLSTATE 07002, wrong parameter count
                return DatabaseTransientException(ex)
            return DatabaseTerminalException(ex)
        if isinstance(ex, mssql_python.OperationalError):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, mssql_python.Error)

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return f"TOP ({limit})", ""


# Backwards-compatible alias: this client now uses the mssql-python driver instead of pyodbc.
PyOdbcMsSqlClient = MsSqlClient
