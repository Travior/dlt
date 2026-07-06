import atexit
import json
import os
import time
from collections.abc import ItemsView
from typing import Any, ClassVar, Dict, List, Optional, Tuple


Scalar = str | int | float | bool | None
Event = Dict[str, Scalar]
CounterKey = Tuple[str, Tuple[Tuple[str, Scalar], ...]]

_START = time.perf_counter()
_DIR = os.environ.get("DLT_BENCH_DIR")
_BUFFER: Optional[List[Event]] = [] if _DIR else None
_COUNTERS: Optional[Dict[CounterKey, int]] = {} if _DIR else None
_PID = os.getpid()
_REGISTERED = False


def enabled() -> bool:
    """Tells if benchmark event collection is active in this process."""
    return _BUFFER is not None


def event(name: str, **fields: Any) -> None:
    """Records a benchmark event when collection is active.

    Args:
        name (str): Event name.
        **fields (Any): JSON-serializable scalar fields attached to the event.

    Raises:
        TypeError: If a field value is not scalar while running in tests.
    """
    if _BUFFER is None:
        return
    event_fields = _coerce_fields(fields.items())
    event_fields.update({"name": name, "t": time.perf_counter() - _START, "pid": _PID})
    _BUFFER.append(event_fields)


def count(name: str, n: int = 1, **fields: Any) -> None:
    """Accumulates a benchmark counter when collection is active.

    Args:
        name (str): Counter event name.
        n (int): Increment added to the counter.
        **fields (Any): Scalar fields that identify the counter series.

    Raises:
        TypeError: If a field value is not scalar while running in tests.
    """
    if _COUNTERS is None:
        return
    counter_fields = tuple(sorted(_coerce_fields(fields.items()).items()))
    key = (name, counter_fields)
    _COUNTERS[key] = _COUNTERS.get(key, 0) + n


class span:
    """Measures duration of a benchmarked block when collection is active."""

    __slots__ = ("_fields", "_name", "_start")

    _NO_START: ClassVar[float] = -1.0

    def __init__(self, name: str, **fields: Any) -> None:
        self._name = name
        self._fields = fields
        self._start = self._NO_START

    def __enter__(self) -> "span":
        if _BUFFER is not None:
            self._start = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        if _BUFFER is None or self._start == self._NO_START:
            return
        event(self._name, duration=time.perf_counter() - self._start, **self._fields)


def _dump() -> None:
    """Writes buffered benchmark events and counters as JSONL without raising."""
    try:
        if _DIR is None or _BUFFER is None or _COUNTERS is None:
            return
        events = list(_BUFFER)
        for (name, fields), value in _COUNTERS.items():
            counter_event: Event = dict(fields)
            counter_event.update(
                {"name": name, "n": value, "t": time.perf_counter() - _START, "pid": _PID}
            )
            events.append(counter_event)
        if not events:
            return
        os.makedirs(_DIR, exist_ok=True)
        path = os.path.join(_DIR, f"{_PID}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for item in events:
                f.write(json.dumps(item, sort_keys=True, separators=(",", ":")))
                f.write("\n")
    except Exception:
        return


def _activate(path: str) -> None:
    """Enables collection in the current process."""
    global _BUFFER, _COUNTERS, _DIR, _REGISTERED, _START

    _DIR = path
    _START = time.perf_counter()
    _BUFFER = []
    _COUNTERS = {}
    if not _REGISTERED:
        atexit.register(_dump)
        _REGISTERED = True


def _deactivate() -> None:
    """Disables collection in the current process."""
    global _BUFFER, _COUNTERS, _DIR

    _DIR = None
    _BUFFER = None
    _COUNTERS = None


def _coerce_fields(fields: ItemsView[str, Any]) -> Event:
    return {key: _coerce_field(value) for key, value in fields}


def _coerce_field(value: Any) -> Scalar:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if os.environ.get("DLT_BENCH_STRICT_FIELDS") == "1" or "PYTEST_CURRENT_TEST" in os.environ:
        raise TypeError(f"Benchmark field values must be scalar, got {type(value).__name__}")
    return str(value)


if _BUFFER is not None:
    atexit.register(_dump)
    _REGISTERED = True
