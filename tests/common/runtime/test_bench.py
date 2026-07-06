import importlib
import json
from pathlib import Path
from typing import Iterator

import pytest

import dlt.common.runtime.bench as bench


@pytest.fixture()
def disabled_bench(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("DLT_BENCH_DIR", raising=False)
    importlib.reload(bench)
    yield
    bench._deactivate()


@pytest.fixture()
def enabled_bench(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("DLT_BENCH_DIR", str(tmp_path))
    monkeypatch.setenv("DLT_BENCH_STRICT_FIELDS", "1")
    importlib.reload(bench)
    yield tmp_path
    bench._deactivate()


def test_bench_disabled_is_noop(disabled_bench: None, tmp_path: Path) -> None:
    assert bench.enabled() is False

    bench.event("ignored", value={"not": "scalar"})
    bench.count("ignored", value={"not": "scalar"})
    with bench.span("ignored", value={"not": "scalar"}):
        pass
    bench._dump()

    assert list(tmp_path.iterdir()) == []


def test_bench_event_dump_format(enabled_bench: Path) -> None:
    assert bench.enabled() is True

    bench.event("writer_rotate", table="events", file_size=123, compressed=True, nothing=None)
    bench._dump()

    events = _read_events(enabled_bench)
    assert len(events) == 1
    assert events[0]["name"] == "writer_rotate"
    assert events[0]["table"] == "events"
    assert events[0]["file_size"] == 123
    assert events[0]["compressed"] is True
    assert events[0]["nothing"] is None
    assert isinstance(events[0]["t"], float)
    assert isinstance(events[0]["pid"], int)


def test_bench_counters_are_materialized_on_dump(enabled_bench: Path) -> None:
    bench.count("rows", 2, table="events")
    bench.count("rows", 3, table="events")
    bench.count("rows", 7, table="issues")
    bench._dump()

    events = sorted(_read_events(enabled_bench), key=lambda item: item["table"])
    assert len(events) == 2
    assert events[0]["name"] == "rows"
    assert events[0]["table"] == "events"
    assert events[0]["n"] == 5
    assert events[1]["name"] == "rows"
    assert events[1]["table"] == "issues"
    assert events[1]["n"] == 7


def test_bench_span_emits_duration(enabled_bench: Path) -> None:
    with bench.span("compress_file", table="events"):
        pass
    bench._dump()

    events = _read_events(enabled_bench)
    assert len(events) == 1
    assert events[0]["name"] == "compress_file"
    assert events[0]["table"] == "events"
    assert isinstance(events[0]["duration"], float)
    assert events[0]["duration"] >= 0.0


def test_bench_rejects_non_scalar_fields_in_tests(enabled_bench: Path) -> None:
    with pytest.raises(TypeError):
        bench.event("bad", value={"not": "scalar"})

    with pytest.raises(TypeError):
        bench.count("bad", value=["not", "scalar"])


def _read_events(path: Path) -> list[dict]:
    files = list(path.glob("*.jsonl"))
    assert len(files) == 1
    return [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]
