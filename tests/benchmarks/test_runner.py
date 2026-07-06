from dlt.common.runtime.benchmark.runner import run
from tests.benchmarks.cases._smoke import smoke  # noqa: F401


def test_runner_executes_case_in_subprocesses() -> None:
    result = run("smoke")

    assert result.runs == 2
    assert result.events["smoke_event"].count == 2
    assert result.counters()["smoke_count.count"] == 1.0
    assert result.counters()["smoke_count.sum(n)"] == 2.0
    assert result.wall.median >= 0.0
