import contextlib
import os
from typing import Any, Callable, Optional

from dlt.common.metrics import DataWriterMetrics
from dlt.common.data_writers.buffered import BufferedDataWriter


class RemoteBufferedWriter(BufferedDataWriter[Any]):
    def __init__(
        self,
        *args: Any,
        fs_client: Any,
        make_remote_path: Callable[[str], str],
        make_remote_url: Callable[[str], str],
        **kwargs: Any,
    ) -> None:
        self.fs_client = fs_client
        self.make_remote_path = make_remote_path
        self.make_remote_url = make_remote_url
        self._remote_path: Optional[str] = None
        self._remote_url: Optional[str] = None
        super().__init__(*args, **kwargs)

    def _reference_file_name(self) -> str:
        base_file_name, _ = os.path.splitext(self._file_name)
        return f"{base_file_name}.reference"

    def _open_writer(self) -> None:
        file_name = os.path.basename(self._file_name)
        self._remote_path = self.make_remote_path(file_name)
        self._remote_url = self.make_remote_url(self._remote_path)
        parent_path = os.path.dirname(self._remote_path)
        if parent_path:
            self.fs_client.makedirs(parent_path, exist_ok=True)
        self._file = self.fs_client.open(self._remote_path, "wb")
        self._writer = self.writer_cls(self._file, caps=self._caps)  # type: ignore[assignment]
        self._writer.write_header(self._current_columns)

    def _abort_remote_file(self) -> None:
        if self._file is not None:
            with contextlib.suppress(Exception):
                discard = getattr(self._file, "discard", None)
                if discard:
                    discard()
            with contextlib.suppress(Exception):
                self._file.close()
        if self._remote_path:
            with contextlib.suppress(Exception):
                self.fs_client.rm(self._remote_path)

    def _flush_and_close_file(
        self, allow_empty_file: bool = False, skip_flush: bool = False
    ) -> DataWriterMetrics:
        if skip_flush:
            if not self._writer:
                return None
            self._abort_remote_file()
            self._writer = None
            self._file = None
            self._file_name = None
            self._remote_path = None
            self._remote_url = None
            self._created = None
            self._last_modified = None
            return None

        if not self._writer:
            self._flush_items(allow_empty_file)
            if not self._writer:
                return None

        self._flush_items(allow_empty_file)
        self._writer.write_footer()
        self._file.flush()
        self._writer.close()
        remote_bytes = self._file.tell()
        self._file.close()

        reference_file_name = self._reference_file_name()
        with open(reference_file_name, "w", encoding="utf-8") as f:
            f.write(self._remote_url)

        metrics = DataWriterMetrics(
            reference_file_name,
            self._writer.items_count,
            remote_bytes,
            self._created,
            self._last_modified,
        )
        self.closed_files.append(metrics)
        self._writer = None
        self._file = None
        self._file_name = None
        self._remote_path = None
        self._remote_url = None
        self._created = None
        self._last_modified = None
        return metrics
