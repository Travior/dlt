from typing import Any, Dict, Type

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
