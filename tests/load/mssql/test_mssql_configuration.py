import os

import pytest

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_mssql_factory() -> None:
    schema = Schema("schema")
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is False
    assert client.config.has_case_sensitive_identifiers is False
    assert client.capabilities.has_case_sensitive_identifiers is False
    assert client.capabilities.casefold_identifier is str

    # MSSQL uses ADBC for parquet loading which doesn't support dictionary-encoded arrays
    assert client.capabilities.parquet_format is not None
    assert client.capabilities.parquet_format.supports_dictionary_encoding is False

    # set args explicitly
    dest = mssql(has_case_sensitive_identifiers=True, create_indexes=True)
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str

    # set args via config
    os.environ["DESTINATION__CREATE_INDEXES"] = "True"
    os.environ["DESTINATION__HAS_CASE_SENSITIVE_IDENTIFIERS"] = "True"
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str


def test_mssql_credentials_defaults() -> None:
    creds = MsSqlCredentials()
    assert creds.port == 1433
    assert creds.connect_timeout == 30
    assert MsSqlCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(creds, explicit_value="mssql://loader:loader@localhost/dlt_data")
    assert creds.port == 1433


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "mssql://user1:pass1@host1:1433/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "mssql://user1:pass1@host1:1434/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_mssql_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = MsSqlCredentials(connection_string)
        config = MsSqlClientConfiguration(credentials=credentials)
    else:
        config = MsSqlClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


def test_parse_native_representation() -> None:
    # Case: password not specified.
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(MsSqlCredentials("mssql://test_user@sql.example.com/test_db"))


def test_to_odbc_dsn() -> None:
    # mssql-python bundles its own driver, so the DSN carries no DRIVER and any `driver`
    # query parameter (legacy pyodbc config) is ignored.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: custom port.
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com:12345/test_db")
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }


def test_to_odbc_dsn_arbitrary_keys_specified() -> None:
    # Arbitrary query keys are passed through (the `driver` key is dropped).
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&BAR=b&Driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }


def test_to_odbc_dsn_connect_timeout_and_longasmax_dropped() -> None:
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?connect_timeout=15&LongAsMax=yes&Encrypt=yes"
        )
    )
    dsn = creds.to_odbc_dsn()
    assert "connect_timeout" not in dsn.lower()
    assert "longasmax" not in dsn.lower()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "ENCRYPT": "yes",
    }
