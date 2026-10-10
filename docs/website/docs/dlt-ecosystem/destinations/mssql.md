---
title: MS SQL
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---
# Microsoft SQL Server

## Install dlt with MS SQL

To install the dlt library with MS SQL dependencies, use:

```sh
pip install "dlt[mssql]"
```

<!--@@@DLT_DESTINATION_CAPABILITIES mssql-->

## Setup guide

### Prerequisites

`dlt` connects with [mssql-python](https://github.com/microsoft/mssql-python), Microsoft's Python driver for SQL Server. It
bundles the _ODBC Driver 18 for SQL Server_, so you don't need to install an ODBC driver on your system.

:::note
[ibis](../../general-usage/dataset-access/dataset.md#ibis) and [dbt](#dbt-support) still connect through `pyodbc` and need a
system ODBC driver. See [ODBC driver for ibis and dbt](#odbc-driver-for-ibis-and-dbt).
:::

### Create a pipeline

1. Initialize a project with a pipeline that loads to MS SQL by running:

```sh
dlt init chess mssql
```

2. Install the necessary dependencies for MS SQL by running:

```sh
pip install -r requirements.txt
```

or run:

```sh
pip install "dlt[mssql]"
```

This will install `dlt` with the `mssql` extra, which contains all the dependencies required by the SQL server client.

3. Enter your credentials into `.dlt/secrets.toml`.

For example, replace with your database connection info:

```toml
[destination.mssql.credentials]
database = "dlt_data"
username = "loader"
password = "<password>"
host = "loader.database.windows.net"
port = 1433
connect_timeout = 15
[destination.mssql.credentials.query]
# trust self-signed SSL certificates
TrustServerCertificate="yes"
# require SSL connection
Encrypt="yes"
```

You can also pass a SQLAlchemy-like database connection:

```toml
# Keep it at the top of your TOML file, before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?TrustServerCertificate=yes&Encrypt=yes"
```

You can place ODBC connection keywords into the query string or **destination.mssql.credentials.query** TOML table as in the example above.
See [connection keywords](#connection-keywords) for the keywords `dlt` passes to the driver.

**To connect to an `mssql` server using Windows authentication**, include `trusted_connection=yes` in the connection string.

```toml
destination.mssql.credentials="mssql://loader.database.windows.net/dlt_data?trusted_connection=yes"
```

:::note
If you encounter missing credentials errors when using Windows authentication, set the 'username' and 'password' as empty strings in the TOML file.
:::

**To connect to a local SQL server instance running without SSL**, pass the `encrypt=no` parameter:

```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?encrypt=no"
```

**To allow a self-signed SSL certificate** when you are getting `certificate verify failed: unable to get local issuer certificate`:

```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?TrustServerCertificate=yes"
```

### Microsoft Entra ID authentication

Set `authentication` to sign in with Microsoft Entra ID instead of a SQL login. `mssql-python` gets the access tokens:

```toml
[destination.mssql.credentials]
database = "dlt_data"
host = "loader.database.windows.net"
authentication = "ActiveDirectoryDefault"
```

| `authentication` | Signs in with | `username` and `password` |
| --- | --- | --- |
| `ActiveDirectoryDefault` | [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential): environment variables, managed identity, Azure CLI and more | not needed |
| `ActiveDirectoryServicePrincipal` | a service principal | client id and client secret |
| `ActiveDirectoryPassword` | an Entra ID user and password | required |
| `ActiveDirectoryMsi` | a managed identity | `username` is the client id of a user-assigned identity |
| `ActiveDirectoryInteractive` | a browser login | `username` is an optional login hint |
| `ActiveDirectoryDeviceCode` | a device code login | not needed |
| `ActiveDirectoryIntegrated` | the Windows or Kerberos login of the process | not needed |
| `SqlPassword` | a SQL login, like when `authentication` is not set | required |

`Authentication` in the query string works as well. `trusted_connection=yes` signs in with the Windows or Kerberos login of the process,
without `username` and `password`.

To sign in with any other identity, pass an azure-identity `TokenCredential` in code. The driver asks it for a token on every connect,
so long loads keep working when tokens expire:

```py
import dlt
from azure.identity import ClientAssertionCredential
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials


def get_assertion() -> str:
    return "<a federated token, e.g. from GitHub Actions OIDC>"


credentials = MsSqlCredentials()
credentials.host = "loader.database.windows.net"
credentials.database = "dlt_data"
credentials.azure_credential = ClientAssertionCredential("<tenant id>", "<client id>", get_assertion)
pipeline = dlt.pipeline(destination=dlt.destinations.mssql(credentials=credentials))
```

`azure_credential` can't be combined with `authentication`, `username`, `password` or the `UID`, `PWD` and `Trusted_Connection` query keys.

### Connection keywords

`dlt` translates the keywords you configure to the ones `mssql-python` accepts:

* `Server`, `Address`, `Addr`, `Database`, `UID`, `PWD`, `Authentication`, `Trusted_Connection`, `Encrypt`, `TrustServerCertificate`,
  `HostnameInCertificate`, `ServerCertificate`, `ServerSPN`, `MultiSubnetFailover`, `ApplicationIntent`, `ConnectRetryCount`,
  `ConnectRetryInterval`, `KeepAlive`, `KeepAliveInterval`, `IpAddressPreference` and `PacketSize` (or `Packet Size`) are passed to the driver.
  Keywords are case-insensitive. When synonyms such as `Server` and `Addr` are both set, the first one wins.
* `Driver` and `LongAsMax` are not needed: the bundled driver binds long strings and binaries as `max` types.
* `DSN`, `FileDSN`, `AttachDBFileName`, `Failover_Partner`, `FailoverPartnerSPN`, `ClientCertificate`, `ClientKey`, the Always Encrypted keywords
  (`ColumnEncryption`, `KeystoreAuthentication`, `KeystorePrincipalId`, `KeystoreSecret`), `Net`/`Network` other than TCP, and
  `AutoTranslate`, `QuotedId` or `AnsiNPW` set to `no` are rejected: `mssql-python` cannot honor them.
* Any other keyword is ignored with a warning, as the ODBC driver did before.
* Keywords with empty values are left out.

:::caution
The ODBC Driver 17 did not encrypt connections by default, the bundled ODBC Driver 18 does. If you configured
`driver="ODBC Driver 17 for SQL Server"` and no `Encrypt` keyword, `dlt` sets `Encrypt=no` for you and warns. Set `Encrypt`
explicitly to silence the warning.
:::

**To pass credentials directly**, use the [explicit instance of the destination](../../general-usage/destination.md#pass-explicit-credentials)

```py
pipeline = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15"),
  dataset_name='chess_data')
```

## Write disposition

All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and
recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: MSSQL supports DDL transactions.

## Data loading

:::tip
Load Arrow tables and pandas frames with **parquet**: `dlt` bulk copies them with `mssql-python`, which is much faster than the INSERT method.
:::

### Fast loading with parquet

[parquet](../file-formats.md#parquet) files are loaded with the Arrow bulk copy of `mssql-python`. It needs `pyarrow`:

```sh
pip install "dlt[mssql,parquet]"
```

When `pyarrow` is installed, **parquet** is supported and Arrow tables and pandas frames are loaded with it. Other data keeps
loading with INSERT statements unless you ask for parquet:

```py
data_iter = iter(())  # your data generator
pipeline.run(data_iter, dataset_name="speed_test_2", write_disposition="replace", table_name="unsw_flow", loader_file_format="parquet")
```

Each file is copied in a single batch over its own connection with the same credentials, so a failed load job commits nothing and can be retried.
Columns are matched by name, and `dlt` checks that every row of the file was copied.

Bulk copy cannot sign in with `Authentication=ActiveDirectoryPassword` or `Authentication=ActiveDirectoryIntegrated`. Load with
`loader_file_format="insert_values"` when you use those.

### Loading with INSERT statements

Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT, and this is what we use. We send multiple
sql statements in a single batch. In case you observe odbc driver locking (i.e. when connection with open transaction leaks into the pool) you can:

1. disable the `mssql-python` connection pool.

```py
import mssql_python
mssql_python.pooling(enabled=False)
```

2. disable batching of multiple statements in `dlt`

```py
dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15", supports_multiple_statements=False)
```

## Supported file formats

* [insert-values](../file-formats.md#sql-insert) is used by default
* [parquet](../file-formats.md#parquet) is supported if `pyarrow` is installed and used for Arrow tables and pandas frames

## Supported column hints

**mssql** will create unique indexes for all columns with `unique` hints. This behavior **is disabled by default**.

### Table and column identifiers

SQL Server **with the default collation** uses case-insensitive identifiers but will preserve the casing of identifiers that are stored in the INFORMATION SCHEMA. You can use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) to keep the identifier casing. Note that you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

If you change the SQL Server server/database collation to case-sensitive, this will also affect the identifiers. Configure your destination as below in order to use case-sensitive naming conventions without collisions:

```toml
[destination.mssql]
has_case_sensitive_identifiers=true
```

## Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Data types

MS SQL does not support JSON columns, so JSON objects are stored as strings in `nvarchar` columns.

## Additional destination options

The **mssql** destination **does not** create UNIQUE indexes by default on columns with the `unique` hint (i.e., `_dlt_id`). To enable this behavior:

```toml
[destination.mssql]
create_indexes=true
```

### ODBC driver for ibis and dbt

ibis and dbt connect through `pyodbc`, which needs the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
installed on your system. ibis uses `ODBC Driver 18 for SQL Server` or `ODBC Driver 17 for SQL Server`, whichever is installed,
and dbt uses `ODBC Driver 18 for SQL Server`. You can set the driver name explicitly:

```toml
[destination.mssql.credentials]
driver="ODBC Driver 18 for SQL Server"
```

When using a SQLAlchemy connection string, replace spaces with `+`:

```toml
# Keep it at the top of your TOML file, before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?driver=ODBC+Driver+18+for+SQL+Server"
```

### dbt support

This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-sqlserver](https://github.com/dbt-msft/dbt-sqlserver).

<!--@@@DLT_TUBA mssql-->
