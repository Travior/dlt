from typing import Any

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.destinations import path_utils


class StagingSpool:
    def __init__(
        self, staging_client: Any, schema_name: str, load_id: str, created_at: Any
    ) -> None:
        self.staging_client = staging_client
        self.schema_name = schema_name
        self.load_id = load_id
        self.created_at = ensure_pendulum_datetime_utc(created_at)
        self.fs_client = staging_client.fs_client

    def make_remote_file_path(self, file_name: str) -> str:
        return path_utils.create_path(
            layout=self.staging_client.config.layout,
            file_name=file_name,
            schema_name=self.schema_name,
            load_id=self.load_id,
            current_datetime=self.staging_client.config.current_datetime,
            load_package_timestamp=self.created_at,
            extra_placeholders=self.staging_client.config.extra_placeholders,
        )

    def make_remote_path(self, file_name: str) -> str:
        remote_file_path = self.make_remote_file_path(file_name)
        pathlib = self.staging_client.config.pathlib
        return pathlib.join(  # type: ignore[no-any-return]
            self.staging_client.dataset_path,
            path_utils.normalize_path_sep(pathlib, remote_file_path),
        )

    def make_remote_url(self, remote_path: str) -> str:
        return self.staging_client.make_remote_url(remote_path)

    def cleanup_load_prefix(self) -> None:
        prefix = self.make_remote_path("_dlt_direct_spool_probe.0.0.parquet")
        marker = f"{self.load_id}{self.staging_client.pathlib.sep}"
        if marker in prefix:
            prefix = prefix[: prefix.index(marker) + len(marker)]
        else:
            return
        self.fs_client.rm(prefix, recursive=True)
