from typing import Any
from unittest.mock import Mock

import mssql_python
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.warnings import DltDeprecationWarning
from dlt.destinations import mssql
from dlt.destinations._adbc_jobs import AdbcParquetCopyJob
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.fabric.configuration import FabricCredentials
from dlt.destinations.impl.fabric.sql_client import FabricSqlClient
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials
from dlt.destinations.impl.mssql.mssql import MssqlParquetCopyJob
from dlt.destinations.impl.mssql.sql_client import MsSqlClient, PyOdbcMsSqlClient
from dlt.destinations.impl.synapse.configuration import SynapseCredentials
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient


@pytest.fixture
def client() -> MsSqlClient:
    return MsSqlClient(
        "dataset", "staging", MsSqlCredentials("mssql://user:pass@host/db"), mssql().capabilities()
    )


@pytest.mark.parametrize("client_type", [MsSqlClient, SynapseSqlClient, FabricSqlClient])
def test_connect(client_type: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    credentials: Any
    if client_type is FabricSqlClient:
        credentials = FabricCredentials()
        credentials.host, credentials.database = "host", "db"
        credentials.azure_client_id = "client"
        credentials.azure_tenant_id = "tenant"
        credentials.azure_client_secret = "secret;with}braces"
        expected_auth = "AUTHENTICATION=ActiveDirectoryServicePrincipal"
    else:
        cls = SynapseCredentials if client_type is SynapseSqlClient else MsSqlCredentials
        credentials = cls("mssql://user:pass@host/db?TrustServerCertificate=yes")
        expected_auth = "UID=user;PWD=pass"
    connect = Mock()
    monkeypatch.setattr(mssql_python, "connect", connect)
    sql_client = client_type("dataset", "staging", credentials, mssql().capabilities())
    assert sql_client.open_connection() is connect.return_value
    connect.assert_called_once_with(
        credentials.to_odbc_dsn(), autocommit=True, timeout=credentials.connect_timeout
    )
    dsn = connect.call_args.args[0]
    assert expected_auth in dsn
    assert "DRIVER=" not in dsn and "longasmax" not in dsn.lower()
    if client_type is FabricSqlClient:
        assert "UID=client@tenant;PWD={secret;with}}braces}" in dsn
    connect.return_value.add_output_converter.assert_not_called()
    sql_client.close_connection()
    connect.return_value.close.assert_called_once()
    assert sql_client.native_connection is None
    assert PyOdbcMsSqlClient is MsSqlClient


@pytest.mark.parametrize(
    "query,args,kwargs,expected",
    [
        ("SELECT 1", (), {}, ("SELECT 1",)),
        ("SELECT %s, %s", (1, "a"), {}, ("SELECT ?, ?", 1, "a")),
        ("SELECT %(value)s", (), {"value": 1}, ("SELECT %(value)s", {"value": 1})),
    ],
)
def test_query_parameters(client: MsSqlClient, query, args, kwargs, expected) -> None:
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.nextset.return_value = False
    client._conn = connection
    with client.execute_query(query, *args, **kwargs):
        pass
    cursor.execute.assert_called_once_with(*expected)
    cursor.close.assert_called_once()


@pytest.mark.parametrize("failure_at", ["execute", "fetch", "nextset", "consumer"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_query_failure_preserves_original_error(
    client: MsSqlClient, failure_at: str, cleanup_fails: bool
) -> None:
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.nextset.return_value = False
    client._conn = connection
    error = mssql_python.ProgrammingError("Syntax error or access violation", "original")
    consumer_error = ValueError("consumer failed")
    if failure_at == "execute":
        cursor.execute.side_effect = error
    elif failure_at == "fetch":
        cursor.fetchall.side_effect = error
    elif failure_at == "nextset":
        cursor.nextset.side_effect = [True, error]
    if cleanup_fails:
        cursor.close.side_effect = mssql_python.OperationalError("cleanup", "close failed")
        connection.rollback.side_effect = mssql_python.OperationalError(
            "cleanup", "rollback failed"
        )

    expected = ValueError if failure_at == "consumer" else DatabaseTerminalException
    with pytest.raises(expected) as exc:
        with client.execute_query("SELECT 1; SELECT 2") as result:
            if failure_at == "fetch":
                result.fetchall()
            elif failure_at == "consumer":
                raise consumer_error

    if failure_at == "consumer":
        assert exc.value is consumer_error
        connection.rollback.assert_not_called()
    else:
        assert isinstance(exc.value, DatabaseTerminalException)
        assert exc.value.dbapi_exception is error
        connection.rollback.assert_called_once()
    cursor.close.assert_called_once()
    assert cursor.nextset.call_count == (2 if failure_at == "nextset" else 0)


def test_query_drains_all_results(client: MsSqlClient) -> None:
    client._conn = Mock()
    cursor = client._conn.cursor.return_value
    cursor.description = None
    cursor.nextset.side_effect = [True, True, False]
    with client.execute_query("SELECT 1; SELECT 2; SELECT 3"):
        pass
    assert cursor.nextset.call_count == 3
    cursor.close.assert_called_once()
    client._conn.rollback.assert_not_called()


def test_query_close_failure_is_not_swallowed(client: MsSqlClient) -> None:
    client._conn = Mock()
    cursor = client._conn.cursor.return_value
    cursor.description = None
    cursor.nextset.return_value = False
    error = mssql_python.OperationalError("close", "connection lost")
    cursor.close.side_effect = error
    with pytest.raises(DatabaseTransientException) as exc:
        with client.execute_query("SELECT 1"):
            pass
    assert exc.value.dbapi_exception is error
    cursor.close.assert_called_once()


def test_mixed_parameters_are_rejected(client: MsSqlClient) -> None:
    client._conn = Mock()
    client._conn.cursor.return_value.nextset.return_value = False
    with pytest.raises(TypeError, match="positional and named"):
        with client.execute_query("SELECT %s, %(value)s", 1, value=2):
            pass
    client._conn.cursor.assert_not_called()


@pytest.mark.parametrize(
    "error,expected",
    [
        (
            mssql_python.ProgrammingError("Base table or view not found", "missing"),
            DatabaseUndefinedRelation,
        ),
        (
            mssql_python.ProgrammingError("Syntax error or access violation", "(15151)"),
            DatabaseUndefinedRelation,
        ),
        (
            mssql_python.ProgrammingError("Syntax error or access violation", "syntax"),
            DatabaseTerminalException,
        ),
        (
            mssql_python.ProgrammingError(
                "Syntax error or access violation",
                "Cannot find the schema 'missing', because it does not exist or you do not have"
                " permission.",
            ),
            DatabaseUndefinedRelation,
        ),
        (
            mssql_python.ProgrammingError(
                "Syntax error or access violation", "Custom type does not exist"
            ),
            DatabaseTerminalException,
        ),
        (
            mssql_python.ProgrammingError("COUNT field incorrect", "count"),
            DatabaseTransientException,
        ),
        (
            mssql_python.OperationalError("Connection failure", "network"),
            DatabaseTransientException,
        ),
        (
            mssql_python.IntegrityError("Integrity constraint violation", "null"),
            DatabaseTerminalException,
        ),
        (
            mssql_python.DataError("Numeric value out of range", "overflow"),
            DatabaseTerminalException,
        ),
    ],
)
def test_exception_mapping(error: Exception, expected: Any) -> None:
    wrapped = MsSqlClient._make_database_exception(error)
    assert type(wrapped) is expected
    assert wrapped.dbapi_exception is error
    assert MsSqlClient.is_dbapi_exception(error)
    assert not MsSqlClient.is_dbapi_exception(ValueError())


@pytest.mark.parametrize("message", ["(111214)", "No corresponding transaction found."])
def test_synapse_rollback(client: MsSqlClient, message: str) -> None:
    client._conn = Mock()
    client._conn.rollback.side_effect = mssql_python.ProgrammingError(
        "Syntax error or access violation", message
    )
    client.rollback_transaction()
    assert client._conn.autocommit is True


def test_unrelated_rollback_error_is_not_suppressed(client: MsSqlClient) -> None:
    client._conn = Mock()
    error = mssql_python.ProgrammingError("Syntax error or access violation", "Error 1112140")
    client._conn.rollback.side_effect = error
    with pytest.raises(DatabaseTerminalException) as exc:
        client.rollback_transaction()
    assert exc.value.dbapi_exception is error


def test_transactions(client: MsSqlClient) -> None:
    client._conn = Mock()
    with client.begin_transaction():
        assert client._conn.autocommit is False
    client._conn.commit.assert_called_once()
    assert client._conn.autocommit is True
    with pytest.raises(ValueError):
        with client.begin_transaction():
            raise ValueError("original")
    client._conn.rollback.assert_called_once()
    client._conn.rollback.side_effect = mssql_python.OperationalError("failure", "network")
    with pytest.raises(DatabaseTransientException):
        client.rollback_transaction()
    assert client._conn.autocommit is True


def test_transaction_rollback_failure_preserves_body_error(client: MsSqlClient) -> None:
    client._conn = Mock()
    client._conn.rollback.side_effect = mssql_python.OperationalError("rollback", "connection lost")
    error = ValueError("original")
    with pytest.raises(ValueError) as exc:
        with client.begin_transaction():
            raise error
    assert exc.value is error
    client._conn.rollback.assert_called_once()


def test_legacy_driver_warning() -> None:
    with pytest.warns(DltDeprecationWarning, match="deprecated and ignored"):
        credentials = resolve_configuration(
            MsSqlCredentials("mssql://user:pass@host/db?driver=obsolete")
        )
    assert "DRIVER=" not in credentials.to_odbc_dsn()


def test_adbc_load_path_preserved() -> None:
    assert issubclass(MssqlParquetCopyJob, AdbcParquetCopyJob)
    credentials = MsSqlCredentials("mssql://user:pass@host/db?Encrypt=no")
    params = MssqlParquetCopyJob.odbc_to_go_mssql_dsn(credentials.get_odbc_dsn_dict())
    assert params["ENCRYPT"] == "disable"
    assert params["UID"] == "user" and params["PWD"] == "pass"


@pytest.mark.parametrize(
    "destination_type,drivers,expected",
    [
        ("mssql", ["ODBC Driver 18 for SQL Server"], "ODBC Driver 18 for SQL Server"),
        ("mssql", ["ODBC Driver 17 for SQL Server"], "ODBC Driver 17 for SQL Server"),
        ("mssql", [], None),
        ("synapse", ["ODBC Driver 17 for SQL Server"], None),
        ("synapse", ["ODBC Driver 18 for SQL Server"], "ODBC Driver 18 for SQL Server"),
    ],
)
def test_ibis_keeps_pyodbc(
    client: MsSqlClient, monkeypatch: pytest.MonkeyPatch, destination_type, drivers, expected
) -> None:
    pyodbc = pytest.importorskip("pyodbc")
    from dlt.helpers.ibis import ibis

    monkeypatch.setattr(pyodbc, "drivers", lambda: drivers)
    connect = Mock()
    monkeypatch.setattr(ibis, "connect", connect)
    job_client = Mock()
    job_client.config.credentials = client.credentials
    job_client.config.destination_type = destination_type
    if expected:
        mssql().create_ibis_backend(job_client)
        assert connect.call_args.kwargs["driver"] == expected
    else:
        with pytest.raises(SystemConfigurationException, match="Ibis MSSQL"):
            mssql().create_ibis_backend(job_client)
