import dataclasses
import warnings
from typing import ClassVar, Any, Final, List, Dict, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.typing import TSecretStrValue
from dlt.common.warnings import DltDeprecationWarning

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


def escape_mssql_odbc_value(value: Optional[str]) -> str:
    """Escape a value for MSSQL ADO/ODBC connection string format.

    ODBC format supports `{value}` syntax where:
      - `}` inside braces must be doubled to `}}`
      - `;` can safely appear inside braces

    To safely handle values with special characters, we use ODBC-style bracing:
    - Values containing `;` or `}` are wrapped in `{}`
    - `}` inside the value is escaped as `}}`

    Args:
        value: The value to escape

    Returns:
        Escaped value safe for use in ADO/ODBC connection string
    """
    if not value:
        return ""
    # if value contains ; or }, use braced syntax with }} escaping
    if ";" in value or "}" in value:
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
    """Deprecated and ignored: mssql-python bundles its driver, so no ODBC driver name is needed."""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        if self.driver:
            warnings.warn(
                DltDeprecationWarning(
                    "`driver` is deprecated and ignored; mssql-python bundles its own driver",
                    since="1.30.0",
                ),
                stacklevel=2,
            )
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query

    def on_partial(self) -> None:
        if not self.is_partial():
            self.resolve()

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        # mssql-python bundles its own driver, so no DRIVER key is emitted.
        params = {
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
        }
        if self.query is not None:
            # Timeout is passed to connect(); LongAsMax is unsupported by mssql-python.
            skip_keys = {"driver", "connect_timeout", "longasmax"}
            params.update(
                {k.upper(): v for k, v in self.query.items() if k.lower() not in skip_keys}
            )
        return params

    def to_odbc_dsn(self) -> str:
        params = self.get_odbc_dsn_dict()
        return build_odbc_dsn(params)


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
