from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, Tuple

import mssql_python
from mssql_python.exceptions import sqlstate_to_exception

from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.exceptions import (
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


def _driver_error(sqlstate: str) -> str:
    # mssql-python exposes no SQLSTATE, only the fixed `driver_error` text it maps each one to
    return sqlstate_to_exception(sqlstate, "").driver_error


UNDEFINED_RELATION_ERROR = _driver_error("42S02")
SYNTAX_OR_ACCESS_ERROR = _driver_error("42000")
WRONG_PARAMETER_COUNT_ERROR = _driver_error("07002")


class PyOdbcMsSqlClient(SqlClientBase[mssql_python.Connection], DBTransaction):
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
        # native_uuid=False returns uniqueidentifier as str, like pyodbc did
        self._conn = mssql_python.connect(
            self.credentials.to_odbc_dsn(),
            autocommit=True,
            timeout=self.credentials.connect_timeout,
            native_uuid=False,
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
        except Exception:
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
            # synapse error 111214: there was nothing to roll back
            message = str(ex)
            if "111214" not in message and "No corresponding transaction found" not in message:
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
        if kwargs:
            raise NotImplementedError("mssql does not support named parameters in queries")
        if args:
            # TODO: this is bad. See duckdb & athena also
            query = query.replace("%s", "?")
        # NOTE: do not convert it into context manager. it does not close the cursor!
        curr = self._conn.cursor()
        try:
            # unpack because empty tuple gets interpreted as a single argument
            curr.execute(query, *args)
            # NOTE: firsts recordset is wrapped in a cursor
            yield DBApiCursorImpl(curr)  # type: ignore[arg-type]
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except mssql_python.Error:
                pass
        except mssql_python.Error:
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except mssql_python.Error:
                pass
            # immediately rollback transaction
            try:
                self._conn.rollback()
            except mssql_python.Error:
                pass
            raise
        finally:
            # clear all pending result sets
            while curr.nextset():
                pass
            # always close cursor
            curr.close()

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, mssql_python.ProgrammingError):
            if ex.driver_error == UNDEFINED_RELATION_ERROR:
                return DatabaseUndefinedRelation(ex)
            if ex.driver_error == SYNTAX_OR_ACCESS_ERROR:
                # error 15151: the schema or object does not exist
                if "because it does not exist" in ex.ddbc_error:
                    return DatabaseUndefinedRelation(ex)
                return DatabaseTransientException(ex)
            if ex.driver_error == WRONG_PARAMETER_COUNT_ERROR:
                return DatabaseTransientException(ex)
        elif isinstance(ex, mssql_python.OperationalError):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, mssql_python.Error)

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return f"TOP ({limit})", ""
