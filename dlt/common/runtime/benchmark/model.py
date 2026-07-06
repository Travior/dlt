from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, NamedTuple, Optional

from dlt.common.typing import TypedDict


class Event(TypedDict, total=False):
    name: str
    t: float
    pid: int
    run: int


class Stat(NamedTuple):
    median: float
    mean: float
    stdev: float
    min: float
    max: float
    samples: tuple[float, ...]
    unit: str = ""

    @classmethod
    def from_samples(cls, samples: Sequence[float], unit: str = "") -> "Stat":
        if not samples:
            raise ValueError("Stat requires at least one sample")
        values = tuple(float(sample) for sample in samples)
        return cls(
            median=median(values),
            mean=mean(values),
            stdev=stdev(values) if len(values) > 1 else 0.0,
            min=min(values),
            max=max(values),
            samples=values,
            unit=unit,
        )

    def __str__(self) -> str:
        return f"{_format_value(self.median, self.unit)} ±{_format_value(self.stdev, self.unit)} (n={len(self.samples)})"


class EventSet:
    def __init__(self, events: Sequence[Event]) -> None:
        self._events = tuple(dict(event) for event in events)

    def __getitem__(self, name: str) -> "EventSet":
        return EventSet([event for event in self._events if event.get("name") == name])

    def where(self, **fields: Any) -> "EventSet":
        return EventSet(
            [
                event
                for event in self._events
                if all(event.get(field) == value for field, value in fields.items())
            ]
        )

    def run(self, n: int) -> "EventSet":
        return self.where(run=n)

    @property
    def count(self) -> int:
        return len(self._events)

    def sum(self, field: str) -> float:
        return sum(_numeric_value(event[field]) for event in self._events if field in event)

    def stats(self, field: str) -> Stat:
        return Stat.from_samples(
            [_numeric_value(event[field]) for event in self._events if field in event],
            _unit_for_field(field),
        )

    def by(self, field: str) -> dict[Any, "EventSet"]:
        grouped: dict[Any, list[Event]] = {}
        for event in self._events:
            if field in event:
                grouped.setdefault(event[field], []).append(event)
        return {key: EventSet(events) for key, events in grouped.items()}

    def per_run(self, agg: Callable[["EventSet"], float]) -> Stat:
        run_numbers = sorted({event["run"] for event in self._events if "run" in event})
        if not run_numbers:
            raise ValueError("EventSet contains no run-tagged events")
        return Stat.from_samples([agg(self.run(run_number)) for run_number in run_numbers])

    def df(self) -> Any:
        from dlt.common.libs.pandas import pandas

        return pandas.DataFrame(self.to_list())

    def to_list(self) -> list[Event]:
        return [dict(event) for event in self._events]

    def names(self) -> set[str]:
        return {event["name"] for event in self._events if "name" in event}


@dataclass(frozen=True)
class RunResult:
    case_name: str
    runs: int
    events: EventSet
    wall: Stat
    cpu: Stat
    peak_rss: Stat
    net: Optional[dict[str, Stat]] = None
    traces: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def counters(self) -> dict[str, float]:
        counters: dict[str, float] = {
            "wall": self.wall.median,
            "cpu": self.cpu.median,
            "peak_rss": self.peak_rss.median,
        }
        if self.net:
            counters.update({key: stat.median for key, stat in self.net.items()})
        for name in sorted(self.events.names()):
            event_set = self.events[name]
            counters[f"{name}.count"] = self._per_run(
                event_set, lambda events: float(events.count)
            ).median
            for field_name in _summable_fields(event_set):
                counters[f"{name}.sum({field_name})"] = self._per_run(
                    event_set, lambda events, field_name=field_name: events.sum(field_name)
                ).median
        return counters

    def _per_run(self, events: EventSet, agg: Callable[[EventSet], float]) -> Stat:
        return Stat.from_samples([agg(events.run(run_index)) for run_index in range(self.runs)])

    def step_durations(self) -> dict[str, Stat]:
        values: dict[str, list[float]] = {}
        for trace in self.traces:
            for step in trace.get("steps", []):
                step_name = step.get("step")
                duration = _step_duration(step)
                if step_name and duration is not None:
                    values.setdefault(step_name, []).append(duration)
        return {key: Stat.from_samples(samples, "s") for key, samples in sorted(values.items())}

    def table(self) -> str:
        rows = [self.case_name, f"  wall      {self.wall}", f"  cpu       {self.cpu}"]
        rows.append(f"  peak_rss  {self.peak_rss}")
        if self.net:
            for key, stat in sorted(self.net.items()):
                rows.append(f"  {key:<9} {stat}")
        return "\n".join(rows)

    def save(self, label: str) -> Path:
        from dlt.common.runtime.benchmark import store

        return store.save(self, label)


def _summable_fields(events: EventSet) -> list[str]:
    names: set[str] = set()
    for event in events.to_list():
        for key, value in event.items():
            if key in ("duration", "name", "pid", "run", "t"):
                continue
            if _is_number(value):
                names.add(key)
    return sorted(names)


def _numeric_value(value: Any) -> float:
    if not _is_number(value):
        raise TypeError(f"Expected numeric value, got {type(value).__name__}")
    return float(value)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def _step_duration(step: dict[str, Any]) -> Optional[float]:
    duration = step.get("duration")
    if _is_number(duration):
        return float(duration)
    started_at = _parse_datetime(step.get("started_at"))
    finished_at = _parse_datetime(step.get("finished_at"))
    if started_at is None or finished_at is None:
        return None
    return (finished_at - started_at).total_seconds()


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _unit_for_field(field: str) -> str:
    if field in ("duration", "wall", "cpu"):
        return "s"
    if field.endswith("size") or field.endswith("bytes") or field in ("peak_rss",):
        return "byte"
    return ""


def _format_value(value: float, unit: str) -> str:
    if unit == "s":
        return f"{value:.3g}s"
    if unit == "byte":
        return _format_bytes(value)
    if value.is_integer():
        return str(int(value))
    return f"{value:.3g}"


def _format_bytes(value: float) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    current = float(value)
    for unit in units:
        if abs(current) < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{current:.0f}{unit}"
            return f"{current:.3g}{unit}"
        current /= 1024.0
