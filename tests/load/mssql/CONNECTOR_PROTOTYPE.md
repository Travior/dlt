# Connector-only mssql-python prototype

Base: [origin/devel](https://github.com/dlt-hub/dlt/commit/3efddd61b9f85592bc71879ad0ede8a82d2de3d6).
Local branch: `exp/mssql-python-connector`.

This changes the SQL connector for MSSQL, Synapse and Fabric to `mssql-python>=1.13.0`.
The existing ADBC parquet job, capability detection, row-group ingestion and type mappings
are unchanged. Ibis continues to open a separate pyodbc connection; its binary datetimeoffset
converter is not compatible with mssql-python's native datetime values. Driver discovery now
happens only for Ibis (18 or 17 for MSSQL, 18 for Synapse).

## Provenance

PR [#4141](https://github.com/dlt-hub/dlt/pull/4141) was inspected at
[43e642d](https://github.com/dlt-hub/dlt/commit/43e642d920c33f51c052bf5c6e8b469f390adfd0).
Its seven-commit prefix belongs to auth PR [#4147](https://github.com/dlt-hub/dlt/pull/4147);
the following sixteen commits include connector work mixed with further auth changes.
The prefix was not imported.

Selected patches were applied and adapted to the baseline, rather than cherry-picking
the auth stack. Contributor commits retain Sam Debruyn's original author name, email
and author date. Commit subjects were normalized to repository rules; no co-author footers
were added. The separate integration commit uses the checkout's configured author identity.

| Source commit | Reused scope |
| --- | --- |
| [87725c2](https://github.com/dlt-hub/dlt/commit/87725c2336a3df4a03872b6e722d0887d9957991) | SQL client, driver removal, dependency switch, MSSQL/Synapse configuration tests; auth-dependent hunks omitted |
| [61ee061](https://github.com/dlt-hub/dlt/commit/61ee061a25f755ef7017fb90960fa2bef8b41534) | Legacy driver deprecation warning only |
| [310f440](https://github.com/dlt-hub/dlt/commit/310f4408b7c0bc218fe374374b6639a8d2d7c9ca) | Dependency minimum 1.13.0 |
| [9a07ccb](https://github.com/dlt-hub/dlt/commit/9a07ccb244291f944f12ff388a7b0a7e32ca64ce) | Terminal syntax-error classification and corresponding shared SQL tests, in two commits |
| [86bb1b9](https://github.com/dlt-hub/dlt/commit/86bb1b9b02bb4c4611305c2eefe88ea7b091087f) | Exception variable fix folded into the preceding adapted test commit; Ibis skips omitted |
| [43e642d](https://github.com/dlt-hub/dlt/commit/43e642d920c33f51c052bf5c6e8b469f390adfd0) | Driver exception types in insert-job tests |

Integration work covers Fabric DSN compatibility/escaping, Ibis driver discovery, preserving
Synapse rollback-error suppression, extending syntax expectations to all three destinations,
mocked connector tests, migration docs and a freshly generated lockfile. The lockfile was
generated with uv 0.9.5 to avoid unrelated marker serialization changes from uv 0.12.9;
both versions accept `uv lock --check`.

## Deliberate exclusions and limitations

- No `authentication`, `access_token`, `azure_credential`, NotebookUtils or token-provider API
  from #4147/#4141; no native Arrow bulk copy from #4357; no unrelated JSON type change.
- Existing SQL-login/query authentication and Fabric service-principal DSN fields are retained.
  Fabric's baseline `DefaultAzureCredential` configuration fallback is unchanged: the baseline
  SQL client did not consume that credential as a token. This prototype does not introduce a
  new token-auth path to repair that pre-existing gap. dbt's credential mapping remains unchanged.
- Live SQL Server, Synapse and Fabric loads were not available: Docker has no running daemon
  in this orb, and no remote test credentials were provided. Native connect was exercised against
  a closed local port and produced the expected network `OperationalError`, not a missing-library
  or connection-string-parser failure. That is not a successful SQL integration test.
- Live datetimeoffset, long Unicode/MAX values, authentication and Ibis reads still need remote
  regression coverage before shipping. ADBC is covered by its existing mocked row-group tests,
  not a live Columnar driver load.

## Reproduce focused checks

Install the development extras with `uv sync --extra mssql --extra synapse --extra fabric
--extra duckdb --group dev --group adbc --group ibis-bare`, then install
`ibis-framework[mssql]==12.0.0` for the optional Ibis checks. This Linux orb also needed
`libodbc2` for pyodbc to import; no system Microsoft ODBC driver was needed for mocked tests.

Use dummy configuration, not real credentials, for the non-connecting factory tests:

```sh
DESTINATION__MSSQL__CREDENTIALS='mssql://user:pass@localhost/db' \
DESTINATION__SYNAPSE__CREDENTIALS='synapse://user:pass@localhost/db' \
uv run --no-sync pytest \
  tests/load/mssql/test_mssql_configuration.py \
  tests/load/mssql/test_mssql_python_client.py \
  tests/load/mssql/test_mssql_table_builder.py \
  tests/load/synapse/test_synapse_configuration.py \
  tests/load/synapse/test_synapse_table_builder.py \
  tests/load/fabric/test_fabric_configuration.py \
  tests/load/fabric/test_fabric_table_builder.py \
  tests/destinations/test_adbc_jobs.py \
  tests/common/configuration/test_credentials.py -q
```

Result: **108 passed**, including Ibis checks, on mssql-python 1.13.0 and 1.14.0.
Targeted mypy, Ruff, Flake8, Black and `uv lock --check` also pass. The shared live SQL test
expectations were updated and type-checked, but were not executed against a server.
