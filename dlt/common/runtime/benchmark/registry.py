from collections.abc import Callable, Sequence
from dataclasses import dataclass
from fnmatch import fnmatch
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dlt.common.runtime.benchmark.ctx import Ctx
else:
    Ctx = Any


@dataclass(frozen=True)
class CaseSpec:
    name: str
    fn: Callable[[Ctx], None]
    runs: int
    warmup: int
    tags: tuple[str, ...]
    isolate: bool
    probe_net: bool
    module: str


_REGISTRY: dict[str, CaseSpec] = {}


def case(
    runs: int = 5,
    warmup: int = 1,
    tags: Sequence[str] = (),
    isolate: bool = True,
    probe_net: bool = False,
) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    def _decorator(fn: Callable[[Ctx], None]) -> Callable[[Ctx], None]:
        spec = CaseSpec(
            fn.__name__, fn, runs, warmup, tuple(tags), isolate, probe_net, fn.__module__
        )
        _REGISTRY[spec.name] = spec
        return fn

    return _decorator


def get_case(name: str) -> CaseSpec:
    if name in _REGISTRY:
        return _REGISTRY[name]
    discovered = discover()
    for spec in discovered:
        if spec.name == name:
            return spec
    matches = [spec.name for spec in discovered if name in spec.name]
    suffix = f" Did you mean: {', '.join(matches[:5])}?" if matches else ""
    raise KeyError(f"Unknown benchmark case {name!r}.{suffix}")


def discover(pattern: str = "*") -> list[CaseSpec]:
    _import_case_modules()
    if pattern.startswith("tag:"):
        tag = pattern[4:]
        return sorted(
            [spec for spec in _REGISTRY.values() if tag in spec.tags], key=lambda s: s.name
        )
    return sorted(
        [spec for spec in _REGISTRY.values() if fnmatch(spec.name, pattern)], key=lambda s: s.name
    )


def _import_case_modules() -> None:
    for cases_dir, package in ((Path.cwd() / "cases", "cases"),):
        if not cases_dir.exists():
            continue
        for path in sorted(cases_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            _import_module(f"{package}.{path.stem}")


def _import_module(module_name: str) -> ModuleType:
    return import_module(module_name)
