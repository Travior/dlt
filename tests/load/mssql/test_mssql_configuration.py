import os
import sys
from types import ModuleType
from typing import Dict

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials
from dlt.destinations.impl.mssql.factory import discover_odbc_driver

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
    # a driver is accepted without checking it, only ibis and dbt connect with it
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+13+for+SQL+Server"
        )
    )
    assert creds.driver == "ODBC Driver 13 for SQL Server"
    # no driver needed
    creds = resolve_configuration(MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com/db"))
    assert creds.driver is None
    # password not specified
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(MsSqlCredentials("mssql://test_user@sql.example.com/test_db"))


def _parse_dsn(dsn: str) -> Dict[str, str]:
    """Parses `dsn` the way mssql-python does, which rejects keywords it does not accept."""
    return _ConnectionStringParser(validate_keywords=True)._parse(dsn)


@pytest.mark.parametrize(
    "query,expected",
    [
        pytest.param(
            "",
            {"server": "sql.example.com,1433", "database": "test_db", "uid": "u", "pwd": "p"},
            id="fields_only",
        ),
        pytest.param(
            "?DRIVER=ODBC+Driver+18+for+SQL+Server&LongAsMax=yes&connect_timeout=5",
            {"server": "sql.example.com,1433", "database": "test_db", "uid": "u", "pwd": "p"},
            id="bundled_driver_keywords_dropped",
        ),
        pytest.param(
            "?Encrypt=yes&TrustServerCertificate=yes&ApplicationIntent=ReadOnly&Packet+Size=8192",
            {
                "server": "sql.example.com,1433",
                "database": "test_db",
                "uid": "u",
                "pwd": "p",
                "encrypt": "yes",
                "trustservercertificate": "yes",
                "applicationintent": "ReadOnly",
                "packetsize": "8192",
            },
            id="supported_keywords_kept",
        ),
        pytest.param(
            "?driver=ODBC+Driver+17+for+SQL+Server",
            {
                "server": "sql.example.com,1433",
                "database": "test_db",
                "uid": "u",
                "pwd": "p",
                "encrypt": "no",
            },
            id="driver_17_keeps_no_encryption",
        ),
        pytest.param(
            "?driver=ODBC+Driver+17+for+SQL+Server&encrypt=yes",
            {
                "server": "sql.example.com,1433",
                "database": "test_db",
                "uid": "u",
                "pwd": "p",
                "encrypt": "yes",
            },
            id="driver_17_explicit_encryption",
        ),
        pytest.param(
            "?ColumnEncryption=Disabled&Network=dbmssocn&QuotedId=yes",
            {"server": "sql.example.com,1433", "database": "test_db", "uid": "u", "pwd": "p"},
            id="unsupported_keywords_with_default_values_dropped",
        ),
    ],
)
def test_to_odbc_dsn(query: str, expected: Dict[str, str]) -> None:
    creds = resolve_configuration(MsSqlCredentials(f"mssql://u:p@sql.example.com/test_db{query}"))
    assert _parse_dsn(creds.to_odbc_dsn()) == expected


def test_to_odbc_dsn_ignores_unknown_keywords() -> None:
    # pyodbc ignored keywords the ODBC driver does not know, mssql-python rejects them
    creds = resolve_configuration(
        MsSqlCredentials("mssql://u:p@sql.example.com:12345/test_db?FOO=a&MARS_Connection=yes")
    )
    with pytest.warns(UserWarning, match="FOO"):
        dsn = creds.to_odbc_dsn()
    assert _parse_dsn(dsn) == {
        "server": "sql.example.com,12345",
        "database": "test_db",
        "uid": "u",
        "pwd": "p",
    }


def test_to_odbc_dsn_first_synonym_wins() -> None:
    creds = resolve_configuration(
        MsSqlCredentials("mssql://u:p@sql.example.com/test_db?addr=other.example.com")
    )
    with pytest.warns(UserWarning, match="ADDR"):
        dsn = creds.to_odbc_dsn()
    assert _parse_dsn(dsn)["server"] == "sql.example.com,1433"


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("?DSN=my_dsn", id="dsn"),
        pytest.param("?ColumnEncryption=Enabled", id="always_encrypted"),
        pytest.param("?AttachDBFileName=db.mdf", id="attach_file"),
        pytest.param("?Network=dbnmpntw", id="named_pipes"),
        pytest.param("?AutoTranslate=no", id="no_char_translation"),
    ],
)
def test_to_odbc_dsn_rejects_unsupported_keywords(query: str) -> None:
    creds = resolve_configuration(MsSqlCredentials(f"mssql://u:p@sql.example.com/test_db{query}"))
    with pytest.raises(ConfigurationValueError, match="not supported by the mssql-python driver"):
        creds.to_odbc_dsn()


def test_to_odbc_dsn_escapes_values() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.username = " user "
    creds.password = "p;a}s{s"
    creds = resolve_configuration(creds)
    assert _parse_dsn(creds.to_odbc_dsn()) == {
        "server": "sql.example.com,1433",
        "database": "test_db",
        "uid": " user ",
        "pwd": "p;a}s{s",
    }


def test_to_odbc_dsn_trusted_connection() -> None:
    # documented way to use windows authentication: empty username and password
    creds = resolve_configuration(
        MsSqlCredentials(
            {
                "host": "sql.example.com",
                "database": "test_db",
                "username": "",
                "password": "",
                "query": {"trusted_connection": "yes"},
            }
        )
    )
    assert _parse_dsn(creds.to_odbc_dsn()) == {
        "server": "sql.example.com,1433",
        "database": "test_db",
        "trusted_connection": "yes",
    }


def test_discover_odbc_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    creds = resolve_configuration(MsSqlCredentials("mssql://u:p@sql.example.com/test_db"))
    pyodbc = ModuleType("pyodbc")
    pyodbc.drivers = lambda: ["ODBC Driver 17 for SQL Server"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyodbc", pyodbc)
    assert discover_odbc_driver(creds) == "ODBC Driver 17 for SQL Server"

    pyodbc.drivers = lambda: []  # type: ignore[attr-defined]
    with pytest.raises(SystemConfigurationException):
        discover_odbc_driver(creds)

    # the configured driver wins
    creds.driver = "ODBC Driver 18 for SQL Server"
    assert discover_odbc_driver(creds) == "ODBC Driver 18 for SQL Server"
