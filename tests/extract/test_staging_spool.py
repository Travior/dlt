import os
import posixpath
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

import fsspec
import pyarrow as pa
import pytest

import dlt
from dlt.common import pendulum
from dlt.common.storages import (
    SchemaStorage,
    SchemaStorageConfiguration,
    NormalizeStorageConfiguration,
)
from dlt.extract import DltSource
from dlt.extract.extract import Extract
from dlt.extract.staging_spool import StagingSpool

from tests.utils import clean_test_storage, get_test_storage_root


def _make_staging_client(tmp_path: Path, fs_client: Any = None) -> Any:
    return SimpleNamespace(
        fs_client=fs_client or fsspec.filesystem("file"),
        pathlib=posixpath,
        dataset_path=(tmp_path / "remote").as_posix(),
        config=SimpleNamespace(
            layout="{schema_name}/{load_id}/{table_name}.{file_id}.{ext}",
            current_datetime=None,
            extra_placeholders=None,
            pathlib=posixpath,
        ),
        make_remote_url=lambda path: f"file://{path}",
    )


def _make_spool(
    tmp_path: Path, fs_client: Any = None, upload_workers: int = 2, load_id: str = "1234567890.101"
) -> StagingSpool:
    staging_client = _make_staging_client(tmp_path, fs_client)
    return StagingSpool(
        staging_client, "test_schema", load_id, pendulum.now(), upload_workers=upload_workers
    )


def _spool_file(spool: StagingSpool, local_dir: Path, file_name: str) -> Any:
    local_path = local_dir / file_name
    local_path.write_text("data", encoding="utf-8")
    remote_path = spool.make_remote_path(file_name)
    reference_path = local_dir / f"{file_name}.reference"
    spool.submit_upload(
        str(local_path), remote_path, spool.make_remote_url(remote_path), str(reference_path)
    )
    return SimpleNamespace(local=local_path, remote=remote_path, reference=reference_path)


def test_staging_spool_uploads_and_drains(tmp_path: Path) -> None:
    local_dir = tmp_path / "local"
    local_dir.mkdir()
    spool = _make_spool(tmp_path)

    jobs = [_spool_file(spool, local_dir, f"items.{idx}.0.parquet") for idx in range(4)]
    spool.drain()

    for job in jobs:
        assert os.path.isfile(job.remote)
        # reference stub carries the remote url
        assert job.reference.read_text(encoding="utf-8") == f"file://{job.remote}"
        # local file was removed after upload
        assert not job.local.exists()
    # spool accepts more uploads after drain
    job = _spool_file(spool, local_dir, "items.5.0.parquet")
    spool.drain()
    assert os.path.isfile(job.remote)


def test_staging_spool_drain_raises_on_failed_upload(tmp_path: Path) -> None:
    local_dir = tmp_path / "local"
    local_dir.mkdir()

    def put_file(local: str, remote: str) -> None:
        raise OSError("upload failed")

    fs_client = SimpleNamespace(
        makedirs=lambda path, exist_ok: None, put_file=put_file, rm=lambda *a, **kw: None
    )
    spool = _make_spool(tmp_path, fs_client=fs_client)
    job = _spool_file(spool, local_dir, "items.0.0.parquet")

    with pytest.raises(OSError, match="upload failed"):
        spool.drain()
    # no reference stub was written for the failed upload
    assert not job.reference.exists()


def test_staging_spool_submit_raises_on_earlier_failure(tmp_path: Path) -> None:
    local_dir = tmp_path / "local"
    local_dir.mkdir()

    def put_file(local: str, remote: str) -> None:
        raise OSError("upload failed")

    fs_client = SimpleNamespace(
        makedirs=lambda path, exist_ok: None, put_file=put_file, rm=lambda *a, **kw: None
    )
    spool = _make_spool(tmp_path, fs_client=fs_client)
    job = _spool_file(spool, local_dir, "items.0.0.parquet")
    # wait for the failure to surface without draining
    while not all(f.done() for f in spool._pending):
        time.sleep(0.01)

    with pytest.raises(OSError, match="upload failed"):
        _spool_file(spool, local_dir, "items.1.0.parquet")
    with pytest.raises(OSError, match="upload failed"):
        spool.drain()
    assert not job.reference.exists()


def test_staging_spool_abort_cleans_remote_prefix(tmp_path: Path) -> None:
    local_dir = tmp_path / "local"
    local_dir.mkdir()
    upload_started = threading.Event()
    release_upload = threading.Event()
    fs = fsspec.filesystem("file")

    def slow_put_file(local: str, remote: str) -> None:
        upload_started.set()
        release_upload.wait(timeout=10)
        fs.put_file(local, remote)

    fs_client = SimpleNamespace(makedirs=fs.makedirs, put_file=slow_put_file, rm=fs.rm)
    spool = _make_spool(tmp_path, fs_client=fs_client, upload_workers=1)
    job = _spool_file(spool, local_dir, "items.0.0.parquet")
    assert upload_started.wait(timeout=10)
    release_upload.set()
    spool.abort()

    # in flight upload finished but the whole load prefix was removed
    assert not os.path.exists(job.remote)
    # aborted spool does not accept new uploads
    with pytest.raises(RuntimeError):
        _spool_file(spool, local_dir, "items.1.0.parquet")


def _make_extract_step() -> Extract:
    clean_test_storage(init_normalize=True)
    schema_storage = SchemaStorage(
        SchemaStorageConfiguration(
            schema_volume_path=os.path.join(get_test_storage_root(), "schemas")
        ),
        makedirs=True,
    )
    return Extract(schema_storage, NormalizeStorageConfiguration())


def test_extract_spools_arrow_to_staging_in_parallel(tmp_path: Path) -> None:
    """Extracts arrow items with direct spool enabled and verifies uploaded files,
    reference stubs in the package and pending uploads drained before commit."""
    fs_client = fsspec.filesystem("file")
    fs = fs_client
    extract_step = _make_extract_step()
    extract_step.config.spool_to_staging = True
    extract_step.set_staging_client(_make_staging_client(tmp_path, fs_client))

    @dlt.resource(write_disposition="append", file_format="parquet")
    def arrow_items() -> Any:
        for idx in range(4):
            yield pa.table({"id": [idx], "value": [f"v{idx}"]})

    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "1"
    try:
        source = DltSource(dlt.Schema("spooled"), "section", [arrow_items()])
        load_id = extract_step.extract(source, 20, 1)
    finally:
        del os.environ["DATA_WRITER__FILE_MAX_ITEMS"]

    # all uploads are done: remote files exist under the load id prefix
    remote_files = fs.glob(f"{(tmp_path / 'remote').as_posix()}/spooled/{load_id}/*.parquet")
    assert len(remote_files) == 4
    # reference stubs are in the new package and point to the remote urls
    package_storage = extract_step.extract_storage.new_packages
    jobs = package_storage.list_new_jobs(load_id)
    reference_jobs = [j for j in jobs if j.endswith(".reference")]
    assert len(reference_jobs) == 4
    for job in reference_jobs:
        with open(package_storage.storage.make_full_path(job), "r", encoding="utf-8") as f:
            assert f.read().startswith("file://")
    # no local parquet files are left in the package
    assert not [j for j in jobs if j.endswith(".parquet")]
    # table was marked for direct spooling
    assert package_storage.get_load_package_state(load_id)["direct_spool_tables"] == ["arrow_items"]


def test_extract_aborts_spool_on_upload_failure(tmp_path: Path) -> None:
    """A failed upload fails the extract and leaves no reference stubs behind."""
    fs = fsspec.filesystem("file")

    def put_file(local: str, remote: str) -> None:
        raise OSError("upload failed")

    fs_client = SimpleNamespace(makedirs=fs.makedirs, put_file=put_file, rm=fs.rm)
    extract_step = _make_extract_step()
    extract_step.config.spool_to_staging = True
    extract_step.set_staging_client(_make_staging_client(tmp_path, fs_client))

    @dlt.resource(write_disposition="append", file_format="parquet")
    def arrow_items() -> Any:
        yield pa.table({"id": [1]})

    source = DltSource(dlt.Schema("spooled"), "section", [arrow_items()])
    with pytest.raises(OSError, match="upload failed"):
        extract_step.extract(source, 20, 1)

    # no reference stubs were written anywhere in extract storage
    package_storage = extract_step.extract_storage.new_packages
    load_id = package_storage.list_packages()[0]
    assert not [j for j in package_storage.list_new_jobs(load_id) if j.endswith(".reference")]
