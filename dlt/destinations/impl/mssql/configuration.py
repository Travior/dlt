import dataclasses
import warnings
from typing import ClassVar, Any, Final, List, Dict, Optional, Tuple

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.typing import TSecretStrValue
from dlt.common.warnings import DltDeprecationWarning

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


def escape_mssql_odbc_value(value: Optional[str]) -> str:
    """Escape a value for MSSQL ADO/ODBC connection string format.

    Values containing `;`, `{` or `}`, or starting or ending with whitespace, are wrapped
    in braces, with `}` doubled.

    Args:
        value: The value to escape

    Returns:
        Escaped value safe for use in ADO/ODBC connection string
    """
    if not value:
        return ""
    if any(c in value for c in ";{}") or value != value.strip():
        return "{" + value.replace("}", "}}") + "}"
    return value


def build_odbc_dsn(params: Dict[str, Any]) -> str:
    """Build an ADO/ODBC connection string for MSSQL, escaping values

    Args:
        params: Dictionary of connection parameters

    Returns:
        ADO/ODBC connection string
    """
    return ";".join(
        f"{k}={escape_mssql_odbc_value(str(v))}" for k, v in params.items() if v is not None
    )


MSSQL_PYTHON_KEYWORDS: Dict[str, str] = {
    "server": "Server",
    "address": "Server",
    "addr": "Server",
    "database": "Database",
    "uid": "UID",
    "pwd": "PWD",
    "authentication": "Authentication",
    "trusted_connection": "Trusted_Connection",
    "encrypt": "Encrypt",
    "trustservercertificate": "TrustServerCertificate",
    "trust_server_certificate": "TrustServerCertificate",
    "hostnameincertificate": "HostnameInCertificate",
    "servercertificate": "ServerCertificate",
    "serverspn": "ServerSPN",
    "multisubnetfailover": "MultiSubnetFailover",
    "applicationintent": "ApplicationIntent",
    "connectretrycount": "ConnectRetryCount",
    "connectretryinterval": "ConnectRetryInterval",
    "keepalive": "KeepAlive",
    "keepaliveinterval": "KeepAliveInterval",
    "ipaddresspreference": "IpAddressPreference",
    "packetsize": "PacketSize",
    "packet size": "PacketSize",
}
"""pyodbc keywords (lowercased) that mssql-python accepts, with their mssql-python spelling"""

IGNORED_PYODBC_KEYWORDS: Tuple[str, ...] = ("driver", "longasmax", "connect_timeout")
"""Keywords dropped without a warning: mssql-python bundles ODBC Driver 18 and binds long values
as max types, `connect_timeout` is passed as the login timeout"""

UNSUPPORTED_PYODBC_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "dsn": (),
    "filedsn": (),
    "attachdbfilename": (),
    "failover_partner": (),
    "failoverpartnerspn": (),
    "columnencryption": ("disabled",),
    "keystoreauthentication": (),
    "keystoreprincipalid": (),
    "keystoresecret": (),
    "clientcertificate": (),
    "clientkey": (),
    "autotranslate": ("yes", "true", "1"),
    "quotedid": ("yes", "true", "1"),
    "ansinpw": ("yes", "true", "1"),
    "net": ("dbmssocn", "tcp"),
    "network": ("dbmssocn", "tcp"),
}
"""Keywords mssql-python cannot honor. Dropping them would change what is read or where it is
read from, so they are rejected unless set to the listed values, which the bundled driver uses"""


def translate_pyodbc_keywords(params: Dict[str, Any]) -> Dict[str, str]:
    """Translates pyodbc connection keywords to the keywords mssql-python accepts.

    Keys are matched case-insensitively. Synonyms are renamed and the first one wins, as in
    ODBC. Empty values and keywords mssql-python does not know are dropped, the latter with a
    warning, because the ODBC driver ignored them.

    Args:
        params: pyodbc connection keywords and their values.

    Returns:
        Dict[str, str]: mssql-python connection keywords and their values.

    Raises:
        ConfigurationValueError: If a keyword mssql-python cannot honor is set.
    """
    keywords: Dict[str, str] = {}
    for key, value in params.items():
        if value is None or str(value) == "":
            continue
        value = str(value)
        name = key.strip().lower()
        if name in UNSUPPORTED_PYODBC_KEYWORDS:
            if value.strip().lower() not in UNSUPPORTED_PYODBC_KEYWORDS[name]:
                raise ConfigurationValueError(
                    f"Connection keyword `{key}` is not supported by the mssql-python driver."
                    " Remove it from the credentials or the connection string query."
                )
            continue
        canonical = MSSQL_PYTHON_KEYWORDS.get(name)
        if canonical is None:
            if name not in IGNORED_PYODBC_KEYWORDS:
                warnings.warn(
                    f"Connection keyword `{key}` is not used by the mssql-python driver and is"
                    " ignored.",
                    UserWarning,
                    stacklevel=2,
                )
            continue
        if canonical in keywords:
            warnings.warn(
                f"Connection keyword `{key}` is ignored because `{canonical}` is already set.",
                UserWarning,
                stacklevel=2,
            )
            continue
        keywords[canonical] = value
    return keywords


def map_pyodbc_keywords(params: Dict[str, Any]) -> str:
    """Builds a connection string mssql-python accepts from pyodbc connection keywords.

    When `ODBC Driver 17 for SQL Server` is configured and `Encrypt` is not, `Encrypt=no` is
    added: driver 17 did not encrypt by default, the bundled driver 18 does.

    Args:
        params: pyodbc connection keywords and their values.

    Returns:
        str: The connection string.

    Raises:
        ConfigurationValueError: If a keyword mssql-python cannot honor is set.
    """
    keywords = translate_pyodbc_keywords(params)
    driver = next((str(v) for k, v in params.items() if k.strip().lower() == "driver" and v), "")
    if "driver 17" in driver.lower() and "Encrypt" not in keywords:
        warnings.warn(
            DltDeprecationWarning(
                "ODBC Driver 17 did not encrypt connections by default and dlt now connects with"
                " the ODBC Driver 18 bundled with mssql-python, which does. `Encrypt=no` is set"
                " for you; set `Encrypt` explicitly in the credentials query",
                since="1.31.0",
            ),
            stacklevel=2,
        )
        keywords["Encrypt"] = "no"
    return build_odbc_dsn(keywords)


@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 1433
    connect_timeout: int = 30
    driver: Optional[str] = None
    """ODBC driver used by ibis and dbt, which connect through pyodbc. The destination connects
    with the ODBC Driver 18 bundled with mssql-python"""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    SUPPORTED_DRIVERS: ClassVar[List[str]] = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]
    """ODBC drivers ibis may connect with, in order of preference"""

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        params = {
            "DRIVER": self.driver,
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
        }
        if self.query is not None:
            params.update({k.upper(): v for k, v in self.query.items()})
        return params

    def to_odbc_dsn(self) -> str:
        """Returns the connection string mssql-python connects with."""
        return map_pyodbc_keywords(self.get_odbc_dsn_dict())


@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: MsSqlCredentials = None

    create_indexes: bool = False
    has_case_sensitive_identifiers: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of the configured host."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    def data_location(self) -> str:
        """Returns host:port."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        port = self.credentials.port or 1433
        return f"{self.credentials.host}:{port}"
