import contextlib
import os
from typing import Any, Callable, Optional

from dlt.common.metrics import DataWriterMetrics
from dlt.common.data_writers.buffered import BufferedDataWriter


class RemoteBufferedWriter(BufferedDataWriter[Any]):
    """Buffered writer that spools closed files to a remote location.

    Items are written to a local file like in the base class. When the file is closed
    or rotated, its upload is scheduled via `submit_upload` and a `.reference` stub
    pointing to the remote url replaces it in the metrics. The stub file is created by
    the upload only after it succeeds, so a missing stub marks an incomplete upload.
    """

    def __init__(
        self,
        *args: Any,
        submit_upload: Callable[[str, str, str, str], None],
        make_remote_path: Callable[[str], str],
        make_remote_url: Callable[[str], str],
        **kwargs: Any,
    ) -> None:
        self.submit_upload = submit_upload
        self.make_remote_path = make_remote_path
        self.make_remote_url = make_remote_url
        super().__init__(*args, **kwargs)

    def _reference_file_name(self, file_name: str) -> str:
        base_file_name, _ = os.path.splitext(file_name)
        return f"{base_file_name}.reference"

    def _flush_and_close_file(
        self, allow_empty_file: bool = False, skip_flush: bool = False
    ) -> DataWriterMetrics:
        file_name: Optional[str] = self._file_name
        if skip_flush:
            metrics = super()._flush_and_close_file(skip_flush=True)
            if metrics is not None:
                # discard the partial file, nothing was uploaded yet
                self.closed_files.pop()
                with contextlib.suppress(OSError):
                    os.remove(file_name)
            return None

        # a file may be opened during flush so take the name after closing
        metrics = super()._flush_and_close_file(allow_empty_file)
        if metrics is None:
            return None
        local_path = metrics.file_path
        remote_path = self.make_remote_path(os.path.basename(local_path))
        remote_url = self.make_remote_url(remote_path)
        reference_file_name = self._reference_file_name(local_path)
        self.submit_upload(local_path, remote_path, remote_url, reference_file_name)
        metrics = metrics._replace(file_path=reference_file_name)
        self.closed_files[-1] = metrics
        return metrics
