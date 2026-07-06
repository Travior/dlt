import json
from pathlib import Path
from typing import Any

from tests.benchmarks.bench.model import EventSet, RunResult


RESULTS_DIR = Path("tests/benchmarks/.results")


def save(result: RunResult, label: str) -> Path:
    path = RESULTS_DIR / label / result.case_name
    path.mkdir(parents=True, exist_ok=True)
    with path.joinpath("events.jsonl").open("w", encoding="utf-8") as f:
        for event in result.events.to_list():
            f.write(json.dumps(event, sort_keys=True, default=str))
            f.write("\n")
    path.joinpath("summary.json").write_text(
        json.dumps(_summary(result), indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    path.joinpath("traces.json").write_text(
        json.dumps(result.traces, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    return path


def load(label: str, case_name: str | None = None) -> dict[str, dict[str, Any]]:
    base = RESULTS_DIR / label
    if case_name:
        return {case_name: _load_case(base / case_name)}
    return {path.name: _load_case(path) for path in sorted(base.iterdir()) if path.is_dir()}


def list_saved() -> dict[str, list[str]]:
    if not RESULTS_DIR.exists():
        return {}
    return {
        label.name: sorted(case.name for case in label.iterdir() if case.is_dir())
        for label in sorted(RESULTS_DIR.iterdir())
        if label.is_dir()
    }


def _summary(result: RunResult) -> dict[str, Any]:
    return {
        "case_name": result.case_name,
        "runs": result.runs,
        "wall": result.wall._asdict(),
        "cpu": result.cpu._asdict(),
        "peak_rss": result.peak_rss._asdict(),
        "net": {key: stat._asdict() for key, stat in (result.net or {}).items()},
        "counters": result.counters(),
        "step_durations": {key: stat._asdict() for key, stat in result.step_durations().items()},
        "meta": result.meta,
    }


def _load_case(path: Path) -> dict[str, Any]:
    summary = json.loads(path.joinpath("summary.json").read_text(encoding="utf-8"))
    events = [
        json.loads(line)
        for line in path.joinpath("events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    traces = json.loads(path.joinpath("traces.json").read_text(encoding="utf-8"))
    return {"summary": summary, "events": EventSet(events), "traces": traces}
