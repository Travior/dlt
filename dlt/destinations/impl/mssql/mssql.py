import time
from typing import Dict, Optional, Sequence, List, Any

from dlt.common import logger
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    LoadJob,
    PreparedTableSchema,
    RunnableLoadJob,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TColumnType

from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.sql_jobs import SqlStagingReplaceFollowupJob, SqlMergeFollowupJob

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration
from dlt.destinations.sql_client import SqlClientBase


HINT_TO_MSSQL_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}
VARCHAR_MAX_N: int = 4000
VARBINARY_MAX_N: int = 8000


class MsSqlStagingReplaceJob(SqlStagingReplaceFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name}")
            # moving staging table to destination schema
            sql.append(
                f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER"
                f" {staging_table_name}"
            )
            # recreate staging table
            sql.append(f"SELECT * INTO {staging_table_name} FROM {table_name} WHERE 1 = 0")
        return sql


class MsSqlMergeJob(SqlMergeFollowupJob):
    @classmethod
    def supports_alias_in_delete(cls) -> bool:
        return False

    @classmethod
    def _to_temp_table(
        cls,
        select_sql: str,
        temp_table_name: str,
        unique_column: str,
        sql_client: SqlClientBase[Any],
    ) -> str:
        return f"SELECT * INTO {temp_table_name} FROM ({select_sql}) as t"

    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        return SqlMergeFollowupJob._new_temp_table_name("#" + table_name, op, sql_client)


class MssqlParquetCopyJob(RunnableLoadJob, HasFollowupJobs):
    """Appends a parquet file to its table with mssql-python's Arrow bulk copy"""

    def run(self) -> None:
        from dlt.common.libs.pyarrow import pyarrow

        sql_client: PyOdbcMsSqlClient = self._job_client.sql_client  # type: ignore[attr-defined]
        casefold = sql_client.capabilities.casefold_identifier
        with pyarrow.parquet.ParquetFile(self._file_path) as parquet_file:
            expected_rows = parquet_file.metadata.num_rows
            schema = parquet_file.schema_arrow
            # stream record batches so memory stays bounded by one batch
            reader = pyarrow.RecordBatchReader.from_batches(schema, parquet_file.iter_batches())
            t_ = time.monotonic()
            rows = sql_client.bulk_copy_arrow(
                self.load_table_name, reader, [casefold(name) for name in schema.names]
            )
        if rows != expected_rows:
            raise LoadJobTerminalException(
                self._file_path,
                f"Bulk copy wrote {rows} rows to {self.load_table_name}, the file has"
                f" {expected_rows}",
            )
        logger.info(
            f"{rows} rows copied from {self._file_name} to {self.load_table_name} in"
            f" {time.monotonic() - t_} s"
        )


class MsSqlJobClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: MsSqlClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        dataset_name, staging_dataset_name = InsertValuesJobClient.create_dataset_names(
            schema, config
        )
        sql_client = PyOdbcMsSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: MsSqlClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}
        self.type_mapper = capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            parsed_file = ParsedLoadJobFileName.parse(file_path)
            if parsed_file.file_format == "parquet":
                job = MssqlParquetCopyJob(file_path)
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [MsSqlMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c, table) for c in new_columns)]

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_destination_type(c, table)

        hints_str = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] == "staging-optimized":  # type: ignore[typeddict-item]
            return [MsSqlStagingReplaceJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
