import contextlib
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, List, Optional

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.destinations import path_utils


class StagingSpool:
    """Builds remote paths for direct spooling and uploads spooled files in parallel.

    Uploads are executed on a bounded thread pool. `submit_upload` applies backpressure
    when too many uploads are in flight. `drain` must be called before the load package
    is committed, `abort` cancels pending uploads and removes remote files.
    """

    def __init__(
        self,
        staging_client: Any,
        schema_name: str,
        load_id: str,
        created_at: Any,
        upload_workers: int = 4,
    ) -> None:
        self.staging_client = staging_client
        self.schema_name = schema_name
        self.load_id = load_id
        self.created_at = ensure_pendulum_datetime_utc(created_at)
        self.fs_client = staging_client.fs_client
        self.upload_workers = max(1, upload_workers)
        self._executor: Optional[ThreadPoolExecutor] = None
        # bounds files awaiting upload so extraction cannot run ahead of the pool
        self._backpressure = threading.BoundedSemaphore(self.upload_workers * 2)
        self._pending: List[Future[None]] = []
        self._lock = threading.Lock()
        self._closed = False

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
        return self.staging_client.make_remote_url(remote_path)  # type: ignore[no-any-return]

    def submit_upload(
        self, local_path: str, remote_path: str, remote_url: str, reference_path: str
    ) -> None:
        """Schedules upload of `local_path` to `remote_path` on the upload pool.

        Blocks when the maximum number of pending uploads is reached. On success the
        upload writes `remote_url` into `reference_path` and deletes `local_path`.
        Raises immediately if a previously scheduled upload failed.
        """
        self._raise_on_failed_uploads()
        self._backpressure.acquire()
        try:
            with self._lock:
                if self._closed:
                    raise RuntimeError(
                        f"Staging spool for load id {self.load_id} is closed and does not accept"
                        " uploads"
                    )
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(
                        self.upload_workers, thread_name_prefix="dlt-spool-upload"
                    )
                self._pending.append(
                    self._executor.submit(
                        self._upload, local_path, remote_path, remote_url, reference_path
                    )
                )
        except Exception:
            self._backpressure.release()
            raise

    def drain(self) -> None:
        """Waits until all pending uploads complete, re-raising the first failure."""
        with self._lock:
            pending = self._pending
            self._pending = []
        try:
            for future in pending:
                future.result()
        finally:
            self._shutdown_executor()

    def abort(self) -> None:
        """Cancels pending uploads, waits for running ones and removes remote files."""
        with self._lock:
            self._closed = True
            pending = self._pending
            self._pending = []
        for future in pending:
            future.cancel()
        self._shutdown_executor()
        self.cleanup_load_prefix()

    def cleanup_load_prefix(self) -> None:
        prefix = self.make_remote_path("_dlt_direct_spool_probe.0.0.parquet")
        marker = f"{self.load_id}{self.staging_client.pathlib.sep}"
        if marker in prefix:
            prefix = prefix[: prefix.index(marker) + len(marker)]
        else:
            return
        self.fs_client.rm(prefix, recursive=True)

    def _upload(
        self, local_path: str, remote_path: str, remote_url: str, reference_path: str
    ) -> None:
        try:
            parent_path = os.path.dirname(remote_path)
            if parent_path:
                self.fs_client.makedirs(parent_path, exist_ok=True)
            self.fs_client.put_file(local_path, remote_path)
            with open(reference_path, "w", encoding="utf-8") as f:
                f.write(remote_url)
            with contextlib.suppress(OSError):
                os.remove(local_path)
        finally:
            self._backpressure.release()

    def _raise_on_failed_uploads(self) -> None:
        with self._lock:
            exceptions = [
                exc for f in self._pending if f.done() and (exc := f.exception()) is not None
            ]
        if exceptions:
            raise exceptions[0]

    def _shutdown_executor(self) -> None:
        with self._lock:
            executor = self._executor
            self._executor = None
        if executor:
            executor.shutdown(wait=True)
