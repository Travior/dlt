from types import SimpleNamespace
from typing import Any, Dict, List, Type

import mssql_python
import pytest
from mssql_python.exceptions import sqlstate_to_exception

from dlt.common.configuration import resolve_configuration
from dlt.destinations import mssql
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.mssql.mssql import MssqlParquetCopyJob
from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "sqlstate,message,expected",
    [
        pytest.param(
            "42S02", "Invalid object name 'TABLE_XXX'.", DatabaseUndefinedRelation, id="no_table"
        ),
        pytest.param(
            "42000",
            "Cannot drop the schema 'unknown', because it does not exist or you do not have"
            " permission.",
            DatabaseUndefinedRelation,
            id="no_schema",
        ),
        pytest.param(
            "42000", "Incorrect syntax near 'SELECTA'.", DatabaseTransientException, id="syntax"
        ),
        pytest.param(
            "07002", "COUNT field incorrect", DatabaseTransientException, id="wrong_arg_count"
        ),
        pytest.param("08S01", "Communication link failure", DatabaseTransientException, id="link"),
        pytest.param("HY000", "General error", DatabaseTransientException, id="general"),
        pytest.param(
            "42S22", "Invalid column name 'column_XXX'.", DatabaseTerminalException, id="no_column"
        ),
        pytest.param(
            "23000", "Cannot insert the value NULL", DatabaseTerminalException, id="integrity"
        ),
    ],
)
def test_make_database_exception(sqlstate: str, message: str, expected: Type[Exception]) -> None:
    ex = sqlstate_to_exception(sqlstate, message)
    assert PyOdbcMsSqlClient.is_dbapi_exception(ex)
    assert type(PyOdbcMsSqlClient._make_database_exception(ex)) is expected


def _client(**query: str) -> PyOdbcMsSqlClient:
    credentials = resolve_configuration(MsSqlCredentials("mssql://u:p@sql.example.com/db"))
    credentials.query = query
    return PyOdbcMsSqlClient("dataset", "dataset_staging", credentials, mssql().capabilities())


def test_open_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: Dict[str, Any] = {}

    def _connect(connection_str: str, **kwargs: Any) -> object:
        calls.update(kwargs, connection_str=connection_str)
        return object()

    monkeypatch.setattr(mssql_python, "connect", _connect)
    _client(encrypt="no").open_connection()
    assert calls == {
        "connection_str": "Server=sql.example.com,1433;Database=db;UID=u;PWD=p;Encrypt=no",
        "autocommit": True,
        "timeout": 30,
        "native_uuid": False,
    }


class _Connection:
    autocommit = False

    def __init__(self, error: Exception) -> None:
        self.error = error

    def rollback(self) -> None:
        raise self.error


def test_rollback_without_transaction_on_synapse() -> None:
    client = _client()
    client._conn = _Connection(  # type: ignore[assignment]
        sqlstate_to_exception(
            "42000",
            "111214;An attempt to complete a transaction has failed. No corresponding transaction"
            " found.",
        )
    )
    client.rollback_transaction()
    assert client._conn.autocommit is True

    client._conn = _Connection(sqlstate_to_exception("42000", "Incorrect syntax"))  # type: ignore[assignment]
    with pytest.raises(DatabaseTransientException):
        client.rollback_transaction()


class _Cursor:
    def __init__(self, rows_copied: int = None) -> None:
        self.rows_copied = rows_copied
        self.calls: List[Dict[str, Any]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def bulkcopy_arrow(self, table_name: str, source: Any, **kwargs: Any) -> Dict[str, Any]:
        rows = source.read_all().num_rows
        self.calls.append(dict(kwargs, table_name=table_name, rows=rows))
        return {"rows_copied": rows if self.rows_copied is None else self.rows_copied}


def _bulk_copy_client(cursor: _Cursor, **query: str) -> PyOdbcMsSqlClient:
    client = _client(**query)
    client._conn = SimpleNamespace(cursor=lambda: cursor)  # type: ignore[assignment]
    return client


def test_bulk_copy_arrow() -> None:
    from dlt.common.libs.pyarrow import pyarrow

    cursor = _Cursor()
    client = _bulk_copy_client(cursor)
    reader = pyarrow.table({"id": [1, 2, 3], "na]me": ["a", "b", "c"]}).to_reader()
    assert client.bulk_copy_arrow("items", reader, ["id", "na]me"]) == 3
    assert cursor.calls == [
        {
            "table_name": "[dataset].[items]",
            "rows": 3,
            "batch_size": 0,
            "timeout": 0,
            "column_mappings": ["id", "na]me"],
            "keep_nulls": True,
        }
    ]


@pytest.mark.parametrize("authentication", ["ActiveDirectoryPassword", "ActiveDirectoryIntegrated"])
def test_bulk_copy_arrow_rejects_authentication(authentication: str) -> None:
    from dlt.common.libs.pyarrow import pyarrow

    cursor = _Cursor()
    client = _bulk_copy_client(cursor, Authentication=authentication)
    with pytest.raises(DatabaseTerminalException, match="Bulk copy cannot sign in"):
        client.bulk_copy_arrow("items", pyarrow.table({"id": [1]}).to_reader(), ["id"])
    assert cursor.calls == []


def _parquet_job(tmp_path: Any, cursor: _Cursor, num_row_groups: int) -> MssqlParquetCopyJob:
    from dlt.common.libs.pyarrow import pyarrow

    file_path = str(tmp_path / "items.1234.0.parquet")
    with pyarrow.parquet.ParquetWriter(file_path, pyarrow.schema([("id", pyarrow.int64())])) as w:
        for group in range(num_row_groups):
            w.write_table(pyarrow.table({"id": [group * 10 + i for i in range(10)]}))
    job = MssqlParquetCopyJob(file_path)
    job._job_client = SimpleNamespace(sql_client=_bulk_copy_client(cursor))  # type: ignore[assignment]
    job._load_table = {"name": "items"}
    return job


def test_parquet_copy_job_streams_file(tmp_path: Any) -> None:
    cursor = _Cursor()
    _parquet_job(tmp_path, cursor, num_row_groups=3).run()
    assert len(cursor.calls) == 1
    assert cursor.calls[0]["rows"] == 30
    assert cursor.calls[0]["column_mappings"] == ["id"]
    assert cursor.calls[0]["table_name"] == "[dataset].[items]"


def test_parquet_copy_job_checks_row_count(tmp_path: Any) -> None:
    with pytest.raises(LoadJobTerminalException, match="wrote 7 rows"):
        _parquet_job(tmp_path, _Cursor(rows_copied=7), num_row_groups=1).run()
