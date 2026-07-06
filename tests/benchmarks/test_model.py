from dlt.common.runtime.benchmark.model import EventSet, RunResult, Stat


def test_stat_from_samples_and_formatting() -> None:
    stat = Stat.from_samples([1.0, 2.0, 30.0], "s")

    assert stat.median == 2.0
    assert stat.mean == 11.0
    assert stat.min == 1.0
    assert stat.max == 30.0
    assert stat.samples == (1.0, 2.0, 30.0)
    assert str(stat) == "2s ±16.5s (n=3)"


def test_event_set_filters_groups_and_aggregates() -> None:
    events = EventSet(
        [
            {"name": "writer_rotate", "run": 0, "table": "events", "file_size": 10},
            {"name": "writer_rotate", "run": 0, "table": "issues", "file_size": 15},
            {"name": "writer_rotate", "run": 1, "table": "events", "file_size": 20},
            {"name": "load_job", "run": 1, "table": "events", "duration": 0.1},
        ]
    )

    assert events["writer_rotate"].count == 3
    assert events.where(table="events").count == 3
    assert events.run(0).sum("file_size") == 25.0
    assert events["writer_rotate"].stats("file_size").median == 15.0
    assert sorted(events.by("table")) == ["events", "issues"]
    assert events["writer_rotate"].per_run(lambda run: run.sum("file_size")).samples == (25.0, 20.0)
    assert events.names() == {"load_job", "writer_rotate"}


def test_run_result_counters_are_deterministic_per_run_medians() -> None:
    result = RunResult(
        case_name="upload_gzip",
        runs=2,
        events=EventSet(
            [
                {"name": "writer_rotate", "run": 0, "file_size": 10, "items_count": 1},
                {"name": "writer_rotate", "run": 0, "file_size": 20, "items_count": 2},
                {"name": "writer_rotate", "run": 1, "file_size": 40, "items_count": 4},
                {"name": "load_job", "run": 0, "duration": 1.0, "retry_count": 0},
            ]
        ),
        wall=Stat.from_samples([1.0, 2.0], "s"),
        cpu=Stat.from_samples([0.5, 0.7], "s"),
        peak_rss=Stat.from_samples([100.0, 200.0], "byte"),
    )

    assert result.counters() == {
        "cpu": 0.6,
        "load_job.count": 0.5,
        "load_job.sum(retry_count)": 0.0,
        "peak_rss": 150.0,
        "wall": 1.5,
        "writer_rotate.count": 1.5,
        "writer_rotate.sum(file_size)": 35.0,
        "writer_rotate.sum(items_count)": 3.5,
    }


def test_run_result_step_durations_from_traces() -> None:
    result = RunResult(
        case_name="upload_gzip",
        runs=2,
        events=EventSet([]),
        wall=Stat.from_samples([1.0], "s"),
        cpu=Stat.from_samples([0.5], "s"),
        peak_rss=Stat.from_samples([100.0], "byte"),
        traces=[
            {"steps": [{"step": "extract", "duration": 1.0}]},
            {
                "steps": [
                    {
                        "step": "extract",
                        "started_at": "2026-07-06T10:00:00+00:00",
                        "finished_at": "2026-07-06T10:00:03+00:00",
                    }
                ]
            },
        ],
    )

    assert result.step_durations()["extract"].samples == (1.0, 3.0)
