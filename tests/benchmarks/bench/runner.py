import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

from tests.benchmarks.bench.ctx import Ctx
from tests.benchmarks.bench.model import Event, EventSet, RunResult, Stat
from tests.benchmarks.bench.registry import CaseSpec, get_case


class CaseFailed(RuntimeError):
    pass


def run(case: str | CaseSpec, runs: Optional[int] = None, save: Optional[str] = None) -> RunResult:
    spec = get_case(case) if isinstance(case, str) else case
    repeat_count = runs or spec.runs
    events: list[Event] = []
    wall_samples: list[float] = []
    cpu_samples: list[float] = []
    rss_samples: list[float] = []
    traces: list[dict[str, Any]] = []
    parent_wall_samples: list[float] = []

    for run_index in range(-spec.warmup, repeat_count):
        result_dir = Path(tempfile.mkdtemp(prefix=f"bench-events-{spec.name}-{run_index}-"))
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"bench-tmp-{spec.name}-{run_index}-"))
        sidecar = _run_worker(spec, run_index, result_dir, tmp_dir)
        if run_index < 0:
            continue
        wall_samples.append(float(sidecar["wall"]))
        cpu_samples.append(float(sidecar.get("cpu", 0.0)))
        rss_samples.append(float(sidecar.get("peak_rss", 0.0)))
        parent_wall_samples.append(float(sidecar.get("parent_wall", 0.0)))
        if sidecar.get("trace"):
            traces.append(sidecar["trace"])
        for event in _read_events(result_dir):
            event["run"] = run_index
            events.append(event)

    result = RunResult(
        case_name=spec.name,
        runs=repeat_count,
        events=EventSet(events),
        wall=Stat.from_samples(wall_samples, "s"),
        cpu=Stat.from_samples(cpu_samples, "s"),
        peak_rss=Stat.from_samples(rss_samples, "byte"),
        traces=traces,
        meta={
            "python": sys.version,
            "platform": platform.platform(),
            "parent_wall": Stat.from_samples(parent_wall_samples, "s")._asdict(),
            "timestamp": time.time(),
        },
    )
    if save:
        result.save(save)
    return result


def _run_worker(spec: CaseSpec, run_index: int, result_dir: Path, tmp_dir: Path) -> dict[str, Any]:
    env = os.environ.copy()
    env["DLT_BENCH_DIR"] = str(result_dir)
    started_at = time.perf_counter()
    cmd = [
        sys.executable,
        "-m",
        "tests.benchmarks.bench.runner",
        "--worker",
        f"{spec.module}:{spec.name}",
        "--run-index",
        str(run_index),
        "--tmp-dir",
        str(tmp_dir),
    ]
    process = subprocess.Popen(cmd, env=env, stderr=subprocess.PIPE, text=True)
    peak_rss = _wait_with_rss(process)
    parent_wall = time.perf_counter() - started_at
    stderr = process.stderr.read() if process.stderr else ""
    if process.returncode != 0:
        raise CaseFailed(stderr)
    sidecar_path = result_dir / "_worker.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["peak_rss"] = peak_rss
    sidecar["parent_wall"] = parent_wall
    return sidecar


def _wait_with_rss(process: subprocess.Popen[str]) -> float:
    try:
        import psutil
    except ImportError:
        process.wait()
        return 0.0
    ps_process = psutil.Process(process.pid)
    peak_rss = 0.0
    while process.poll() is None:
        try:
            peak_rss = max(peak_rss, float(ps_process.memory_info().rss))
        except psutil.Error:
            pass
        time.sleep(0.05)
    return peak_rss


def _read_events(path: Path) -> list[Event]:
    events: list[Event] = []
    for file_path in sorted(path.glob("*.jsonl")):
        for line in file_path.read_text(encoding="utf-8").splitlines():
            events.append(json.loads(line))
    return events


def _worker_main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("case")
    parser.add_argument("--run-index", type=int, required=True)
    parser.add_argument("--tmp-dir", required=True)
    args = parser.parse_args(argv)
    module_name, case_name = args.case.split(":", 1)
    __import__(module_name)
    spec = get_case(case_name)
    ctx = Ctx.create(args.run_index, case_name, Path(args.tmp_dir))
    started_cpu = time.process_time()
    started_wall = time.perf_counter()
    try:
        spec.fn(ctx)
        wall = time.perf_counter() - started_wall
        cpu = time.process_time() - started_cpu
        sidecar = {"wall": wall, "cpu": cpu, "trace": ctx.last_trace()}
        Path(os.environ["DLT_BENCH_DIR"]).joinpath("_worker.json").write_text(
            json.dumps(sidecar, default=str), encoding="utf-8"
        )
    finally:
        ctx.cleanup()


def main(argv: Optional[list[str]] = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if args and args[0] == "--worker":
        _worker_main(args[1:])
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("case")
    parser.add_argument("--runs", type=int, default=None)
    parser.add_argument("--save", default=None)
    ns = parser.parse_args(args)
    print(run(ns.case, runs=ns.runs, save=ns.save).table())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
