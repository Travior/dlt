import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, ContextManager, Optional

from dlt.common.runtime import bench as runtime_bench


class Ctx:
    """Per-run resources handed to a benchmark case body."""

    def __init__(self, tmp_dir: Path, run_index: int, case_name: str) -> None:
        self.tmp_dir = tmp_dir
        self.run_index = run_index
        self.case_name = case_name
        self._pipelines: list[Any] = []

    @classmethod
    def create(cls, run_index: int, case_name: str, tmp_dir: Optional[Path] = None) -> "Ctx":
        path = tmp_dir or Path(tempfile.mkdtemp(prefix=f"bench-{case_name}-{run_index}-"))
        path.mkdir(parents=True, exist_ok=True)
        return cls(path, run_index, case_name)

    def cleanup(self) -> None:
        if os.environ.get("BENCH_KEEP") == "1":
            return
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def pipeline(self, destination: str = "duckdb", **kwargs: Any) -> "Any":
        import dlt

        pipeline_name = kwargs.pop("pipeline_name", f"bench_{self.case_name}_{self.run_index}")
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            pipelines_dir=str(self.tmp_dir / "pipelines"),
            **kwargs,
        )
        self._pipelines.append(pipeline)
        return pipeline

    def timer(self, name: str) -> ContextManager[None]:
        return runtime_bench.span(name)

    def last_trace(self) -> Optional[dict[str, Any]]:
        for pipeline in reversed(self._pipelines):
            trace = getattr(pipeline, "last_trace", None)
            if trace is not None:
                return trace.asdict()
        return None
