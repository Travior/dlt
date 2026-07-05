# Technical Proposal: Extract-Time Spooling to Staging Destination ("Direct Spool")

## Goal

For strongly-typed Arrow/Parquet data run on pipelines with a bucket staging (e.g., Snowflake, BigQuery, Databricks, etc.), stream Parquet bytes **directly to the staging bucket during extraction** instead of:
1. Writing a local Parquet file during extraction.
2. Reading and re-writing the Parquet file during normalization (to add system columns/checks).
3. Reading and uploading the local Parquet file to the staging bucket during load.

Instead, the local extraction package will carry tiny `.reference` job stubs pointing to the eagerly uploaded bucket files. 

### Why this is safe

No semantic changes are introduced to the core dlt package lifecycle. The package still commits atomically, the load step still performs its standard pre-load database schema migrations, and write-disposition barriers are preserved because data visibility in the target warehouse is still fully controlled by the `COPY INTO`/merge SQL steps executed during the load step. Eagerly uploaded bucket data remains completely invisible in the target tables until the load step's transaction completes.

---

## Architectural Context

### 1. Existing Staged Loading Flow
- **Extract:** `ArrowExtractor` writes local Parquet files via `ExtractorItemStorage` (`dlt/extract/storage.py:50`) using `BufferedDataWriter` (`dlt/common/data_writers/buffered.py`).
- **Normalize:** `ArrowItemsNormalizer` (`dlt/normalize/items_normalizers/arrow.py:174`) decides whether to rewrite the file (e.g., if adding `_dlt_id` or format translation is required) or perform a pass-through.
- **Load:** `FilesystemLoadJob` (`dlt/destinations/impl/filesystem/filesystem.py:124`) uploads local files to a structured remote path under the staging directory. Once finished, it emits a `ReferenceFollowupJobRequest` (`dlt/destinations/job_impl.py:129`), which is a text file containing the remote URL. The final database destination processes this stub as a COPY job.

### 2. Direct Spool Flow
- **Extract:** `ArrowExtractor` uses a new `RemoteBufferedWriter` that writes Parquet files locally and, when a file closes (by item/byte size rotation), hands it to a parallel upload pool on `StagingSpool`. The upload writes a tiny `.reference` file containing the remote URL directly into the local extracted package and deletes the local Parquet file, so extraction and uploads overlap and multiple files upload concurrently. All uploads are drained before the package commits.
- **Normalize:** A lightweight pass-through handler skips traditional normalization for `.reference` files and moves them directly to the `new_jobs` loading folder.
- **Load:** The loader processes `.reference` files identically to existing reference jobs, resolving URLs and triggering the destination warehouse's `COPY` commands.

---

## Detailed Design & Work Packages

### WP1 — `RemoteBufferedWriter` (parallel upload on rotation)
**Target Files:** `dlt/common/data_writers/remote.py`.

Subclass `BufferedDataWriter` that writes locally and uploads asynchronously:
- **Local Serialization:** Items are written to a local Parquet file exactly like the base class — no changes to buffering or rotation logic.
- **`_flush_and_close_file()` Override:** Upon file completion (close or rotation via `file_max_items`/`file_max_bytes`), the closed local file is handed to the `StagingSpool` upload pool via `submit_upload(local_path, remote_path, remote_url, reference_path)`. The upload task copies the file to the bucket, writes the local `.reference` stub (carrying the remote URL) and deletes the local Parquet file. Because the stub is written only after a successful upload, a missing stub always marks an incomplete upload.
- **Metrics:** Report `DataWriterMetrics` with `file_path` pointing to the local `.reference` stub so extract metrics can parse job IDs correctly; byte count equals the local (= remote) file size.
- **Failure Abort:** If closed in an exception context (`close(skip_flush=True)`), the partial local file is deleted and no upload is scheduled. Remote cleanup is owned by `StagingSpool.abort()`.
- **Rotation Requirement:** Upload parallelism only materializes when files rotate. `file_max_items`/`file_max_bytes` (or the destination's `recommended_file_size`) must be configured for large tables.

### WP2 — `StagingSpool` Path Builder & Upload Pool
**Target Files:** `dlt/extract/staging_spool.py`.

A helper instantiated per `(schema, load_id)` during extraction (reused across repeated extracts for the same load id so pending uploads are tracked in one place):
- Reuses the staging `FilesystemClient` configuration (`dlt/destinations/impl/filesystem/filesystem.py:144-173`) to instantiate the remote client with the source schema.
- Reuses `path_utils.create_path` to build remote paths that are byte-identical to what the loader would construct (utilizing layout, schema, load ID, and package `created_at` timestamp).
- Exposes `make_remote_url(local_job_file_name) -> str` and the authenticated `fsspec` filesystem instance.
- **Upload Pool:** Owns a `ThreadPoolExecutor` (`extract.spool_upload_workers`, default 4) executing uploads in parallel. `submit_upload` applies backpressure via a bounded semaphore (2x workers) so extraction cannot produce local files faster than the pool drains them, and fails fast if a previously scheduled upload failed. `drain()` waits for all pending uploads and re-raises the first failure — it is called in `manage_writers` after writers close and before the package may be committed. `abort()` cancels pending uploads, waits for in-flight ones and removes the remote load-id prefix.
- **Graceful Fallback:** If staging credentials or client configuration cannot be resolved during extraction, log a warning and fall back to the normal local spooling path.

### WP3 — Eligibility Gate & Extractor Routing
**Target Files:** `dlt/extract/extract.py`, `dlt/extract/extractors.py`, `dlt/extract/storage.py`, `dlt/pipeline/pipeline.py`.

- **Configuration:** Expose a new global config flag `extract.spool_to_staging: bool = False` (default off).
- **Setup in Pipeline:** When `spool_to_staging` is enabled and a staging destination is configured, resolve the staging credentials during the extract step, initialize `StagingSpool`, and pass it to the `Extract` step wrapper.
- **Extraction Routing:** At extraction startup, assess resource eligibility per table:
  1. Item format must be `arrow`.
  2. Write disposition is `append` or `replace` (v1 scope).
  3. Table schema is complete up-front (explicit columns defined in the source, or reflected from `sql_database`, so schema contracts do not require open-ended evolution).
  4. No `add_dlt_id` requirement (if `add_dlt_load_id` is configured, append the column at write time using Arrow compute APIs in the extractor instead of during normalization).
- **Schema-Drift Guard:** If an Arrow batch introduces a column change mid-stream, raise a terminal error pointing users to schema contracts or directing them to disable direct spooling. Do not attempt a mid-table fallback to local files.

### WP4 — Normalize Pass-Through
**Target Files:** `dlt/normalize/worker.py`, `dlt/common/data_writers/writers.py`.

- Because `.reference` is a `TJobFileFormat` and not a standard `LOADER_FILE_FORMAT` (`writers.py:131`), add an explicit branch in the normalize worker (`w_normalize_files`):
  - If the extracted file has a `.reference` extension, bypass item storage normalizers.
  - Verbatim-copy the stub to the `new_jobs` directory inside the load package.
  - Return minimal `DataWriterMetrics` based on the stub size and the recorded item count so loaders and metrics progress loops remain functional.
  - Ensure the table is registered in `seen-data` (`normalize.py:259-267`) so metadata tasks and tables are initialized correctly.

### WP5 — Load step: Staging Truncation Guard
**Target Files:** `dlt/load/load.py`, `dlt/load/utils.py`.

- **The Problem:** The load initialization step (`initialize_package`) triggers table directory truncation on the staging destination (`truncate_tables_on_staging_destination_before_load`, defaulting to `True`, `client.py:333`). Running this step would wipe eagerly uploaded files prior to executing the COPY SQL command.
- **The Fix:** Have the extract step write an explicit marker into the package state, e.g., `direct_spool_tables: List[str]`. Inside `Load.initialize_package`, modify the truncation predicate (`should_truncate_table_before_load_on_staging_destination`) to exclude tables present in the direct spool set.

### WP6 — Failure Handling & Orphan Cleanup
**Target Files:** `dlt/extract/extract.py` (`manage_writers`), `dlt/cli/pipeline_command.py`.

- **Writer Abort:** Implement robust cleanup inside the extractor's exception managers to ensure interrupted write buffers are aborted.
- **Extract Failure:** If extraction fails (including a failed upload surfaced by `drain()`), `manage_writers` calls `StagingSpool.abort()`: pending uploads are cancelled, in-flight ones awaited, and remote objects under the active `load_id` prefix are deleted best-effort.
- **Vacuum Utilities:** Extend `dlt pipeline drop-pending-packages` to sweep and vacuum staging buckets of any orphaned files matching the target `load_id` namespace.

---

## Testing & Validation Plan

### 1. Verification Matrix
Test across Snowflake, BigQuery, and Databricks destinations with GCS/S3/Azure staging configurations:
- **Append Disposition:** Verify files spool directly, load correctly, and state is preserved.
- **Replace Disposition:** Verify staging folders are not cleared prematurely, old tables are replaced correctly on the final destination, and empty-table fallbacks behave normally.
- **Failure Recoverability:** Force kill the process mid-stream. Verify that finalized remote files persist, the `.reference` stub remains in the package, and a pipeline resume successfully triggers COPY.

### 2. Performance Benchmark
Measure wall time and disk footprint against:
- Data source: large table read via `sql_database` (Arrow backend).
- Target: Snowflake with S3 staging.
- Goal: Maximum overlap of extract/upload network operations; transient local disk footprint bounded by in-flight uploads (backpressure semaphore) times rotation file size.

---

## Rollout & Technical Recommendations

1. **Gate Fail-Closed:** If credentials, layouts, or schemas are incompatible, direct spooling should fall back seamlessly to the standard E/N/L pipeline with a warning, avoiding crashes for end-users.
2. **Disk Considerations:** Closed files wait on local disk until uploaded. The backpressure semaphore (2x `spool_upload_workers`) bounds the transient footprint; size `file_max_items`/`file_max_bytes` accordingly on disk-constrained workers.
3. **Merge/SCD2 in v1:** Defer merge/SCD2 write dispositions to a v2 iteration. Verify v1 append and replace stability first. Keep an experimental merge spike test in `tests/load/` to evaluate how `_dlt_id` calculations interact with the eager upload pipeline.
