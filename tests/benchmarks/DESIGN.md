# dlt benchmark harness — design

Status: design approved, not yet implemented.

A thin, two-part benchmarking harness for dlt internals performance work:

- **Emitter** (`dlt/common/runtime/bench.py`): a tiny, zero-dependency event
  collector that ships with dlt. Instrumentation points in dlt internals push
  events into it. Disabled (single `is None` check) unless activated via env var.
- **Harness** (`tests/benchmarks/bench/`): dev-only tooling that runs benchmark
  cases in isolated subprocesses, collects events + process metrics, persists
  results, and diffs them against baselines with significance testing.

The harness lives in-repo so benchmarks stay versioned with the internals they
poke. Cross-branch comparison works via process isolation: the harness runs each
case as a subprocess inside any worktree, so "compare devel vs feature branch"
is: run + `--save` in each worktree, then `bench compare`.

## Layout

```
dlt/common/runtime/bench.py          # emitter — ships with dlt, ~80 lines, zero deps
tests/benchmarks/
    bench/
        __init__.py                  # public API re-exports: case, run, compare, capture, Ctx
        model.py                     # Event, Stat, EventSet, RunResult, ComparisonResult
        registry.py                  # @case decorator, CaseSpec, discovery
        ctx.py                       # Ctx — per-run resources for case bodies
        runner.py                    # orchestration: run(), subprocess worker protocol
        probe.py                     # outside-in process metrics (RSS, CPU, net)
        capture.py                   # inline capture() context manager
        store.py                     # persistence of saved results
        compare.py                   # baseline diffing + significance
        cli.py                       # `bench run|compare|list|show` entry point
    cases/
        compression.py               # reference case(s)
    conftest.py                      # pytest marker so nothing runs in normal CI
    .results/                        # saved results, gitignored
```

## 1. Emitter — `dlt/common/runtime/bench.py`

The only piece inside `dlt/`. No imports from dlt itself: it must be importable
from anywhere (data writers, normalize workers) without cycles.

**Activation:** if env var `DLT_BENCH_DIR` is set at import time, events are
buffered in memory and flushed to `{DLT_BENCH_DIR}/{pid}.jsonl` via `atexit`.
Worker processes (normalize process pool) inherit the env var and write their
own file — no IPC, no locks; the harness merges files afterward. When disabled,
every call is a single `is None` check, cheap enough that instrumentation
points stay in the codebase permanently.

```python
def enabled() -> bool:
    """True if event collection is active in this process."""

def event(name: str, **fields: Any) -> None:
    """Record one event: (name, t_rel, fields).

    t_rel = time.perf_counter() - process start offset.
    fields must be JSON-serializable scalars (str/int/float/bool/None);
    non-scalars raise in tests, are str()-coerced at runtime (never crash dlt).
    No-op when disabled.
    """

def count(name: str, n: int = 1, **fields: Any) -> None:
    """Accumulate a counter keyed by (name, sorted(fields.items())).

    For per-item hot paths where one event per item would distort timing.
    Flushed as synthetic events {name, n=total, **fields} at dump time.
    No-op when disabled.
    """

class span:
    """Context manager measuring a duration.

    with span("compress_file", table="events"):  ->
        event("compress_file", duration=<float s>, table="events")
    __slots__, perf_counter based. No-op (but still a valid CM) when disabled.
    """
    def __init__(self, name: str, **fields: Any) -> None: ...
    def __enter__(self) -> "span": ...
    def __exit__(self, *exc: Any) -> None: ...

def _dump() -> None:
    """atexit hook: write events + materialized counters as JSONL.

    Each line: {"name": ..., "t": <float>, "pid": <int>, **fields}.
    Single open/write/close; never raises.
    """
```

**Field-name conventions (load-bearing — `compare` and `counters()` depend on
them, freeze early):** `table`, `resource`, `file_size`, `items_count`,
`duration`, `load_id`. Events carrying `duration` are treated as timings
(medians, significance-tested); everything else as counters (exact diffing).

**Hot-path rule:** no `bench.event()` in per-row loops. Accumulate locally (or
use `bench.count`) and emit one summary event per file/job.

## 2. Data model — `bench/model.py`

```python
class Event(TypedDict, total=False):
    """One emitted event, merged from worker JSONL files."""
    name: str
    t: float            # seconds since process start
    pid: int
    run: int            # which repetition produced it (added by runner)
    # ... arbitrary user fields


class Stat(NamedTuple):
    """Aggregate of one numeric metric across runs."""
    median: float
    mean: float
    stdev: float
    min: float
    max: float
    samples: tuple[float, ...]   # raw per-run values, kept for significance tests
    unit: str = ""               # "s", "byte", "" — drives formatting

    @classmethod
    def from_samples(cls, samples: Sequence[float], unit: str = "") -> "Stat": ...
    def __str__(self) -> str:
        """'3.42s ±0.11 (n=5)' — humanized via unit."""


class EventSet:
    """Immutable view over a list of Events with aggregation sugar.

    Filters return new EventSets; terminal ops return scalars/dicts.
    ~50 lines of sugar over a list of dicts, with a .df() escape hatch —
    not a query engine.
    """
    def __init__(self, events: Sequence[Event]) -> None: ...

    # -- filtering --
    def __getitem__(self, name: str) -> "EventSet":
        """Filter by event name: r.events['file_upload']."""
    def where(self, **fields: Any) -> "EventSet":
        """Filter by field equality: ev.where(table='events', run=0)."""
    def run(self, n: int) -> "EventSet":
        """Events from repetition n only."""

    # -- terminal aggregations --
    @property
    def count(self) -> int: ...
    def sum(self, field: str) -> float: ...
    def stats(self, field: str) -> Stat: ...
    def by(self, field: str) -> dict[Any, "EventSet"]:
        """Group: ev.by('table') -> {'events': EventSet, ...}."""
    def per_run(self, agg: Callable[["EventSet"], float]) -> Stat:
        """Apply agg to each run's slice, return Stat over runs.
        ev.per_run(lambda e: e.sum('file_size')) -> bytes-per-run distribution."""

    # -- escape hatches --
    def df(self) -> "pandas.DataFrame": ...
    def to_list(self) -> list[Event]: ...
    def names(self) -> set[str]:
        """Distinct event names present — for discovery/printing."""


class RunResult:
    """Everything measured for one case across N repetitions."""
    case_name: str
    runs: int
    events: EventSet                 # all runs merged; 'run' field distinguishes
    wall: Stat                       # per-run wall time (worker-measured, fn body only)
    cpu: Stat                        # user+sys CPU seconds
    peak_rss: Stat                   # bytes
    net: Optional[dict[str, Stat]]   # {'bytes_sent': Stat, 'bytes_recv': Stat} if probed
    traces: list[dict]               # dlt PipelineTrace.asdict() per run, if available
    meta: dict                       # git rev, branch, python version, platform,
                                     # timestamp, dlt version, parent-measured wall

    def counters(self) -> dict[str, float]:
        """Canonical scalar summary used by compare/store:
        {'wall': median, 'peak_rss': median,
         'file_upload.count': ..., 'file_upload.sum(file_size)': ...}
        Auto-derived: for every event name, count; for every numeric field
        on that event, sum — computed per run then medianed. Deterministic keys.
        """
    def step_durations(self) -> dict[str, Stat]:
        """{'extract': Stat, 'normalize': Stat, 'load': Stat} from traces.
        Fed into compare as first-class timing rows."""
    def table(self) -> str:
        """Human-readable single-result report."""
    def save(self, label: str) -> Path:
        """Persist via store.save(); returns path. Convenience for CLI --save."""
```

## 3. Case definition — `bench/registry.py` + `bench/ctx.py`

```python
# registry.py

@dataclass
class CaseSpec:
    name: str                       # function __name__
    fn: Callable[["Ctx"], None]
    runs: int
    warmup: int
    tags: tuple[str, ...]
    isolate: bool                   # True: each run = fresh subprocess (default)
    probe_net: bool                 # enable socket counting shim in worker
    module: str                     # for discovery / re-import in worker

_REGISTRY: dict[str, CaseSpec]

def case(
    runs: int = 5,
    warmup: int = 1,
    tags: Sequence[str] = (),
    isolate: bool = True,
    probe_net: bool = False,
) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    """Register a benchmark case.

    The decorated function receives a Ctx and executes the workload, nothing
    else — no timing, no assertions. Returns fn unchanged (so it stays
    directly callable/testable).
    """

def get_case(name: str) -> CaseSpec:
    """Lookup, with fuzzy 'did you mean' error."""

def discover(pattern: str = "*") -> list[CaseSpec]:
    """Import all modules under tests/benchmarks/cases/, return matching specs
    (glob on name, or 'tag:<tag>')."""
```

```python
# ctx.py

class Ctx:
    """Per-run resources handed to a case body. Created fresh for every
    repetition; owns cleanup (tmp dirs removed unless BENCH_KEEP=1)."""

    tmp_dir: Path            # fresh, empty; also used as pipelines_dir
    run_index: int           # 0-based repetition number; warmup runs get -1

    def pipeline(self, destination: str = "duckdb", **kwargs: Any) -> "dlt.Pipeline":
        """Preconfigured pipeline: unique name, pipelines_dir under tmp_dir,
        dev_mode-ish isolation, progress disabled. Destinations resolve to
        tmp_dir-local storage (duckdb file / filesystem bucket)."""

    def data(self, case_name: str) -> Iterator[dict]:
        """Canned datasets by short name: 'github.events', 'github.issues',
        'ethereum.blocks'. Loads from tests/normalize/cases via dlt json.
        Fresh iterator each call."""

    def schema(self, name: str) -> "Schema":
        """Canned schemas, e.g. 'ethereum' — for warm-path normalizer cases."""

    def rows(self, n: int, shape: str = "flat", seed: int = 42) -> Iterator[dict]:
        """Synthetic data via mimesis, deterministic per seed.
        shapes: 'flat' (30 scalar cols, ISO timestamps), 'nested' (2-level,
        lists of dicts -> child tables), 'wide' (200 cols)."""

    def timer(self, name: str) -> ContextManager[None]:
        """Sugar for bench.span() so cases can mark sub-phases:
        with ctx.timer('extract_only'): pipeline.extract(...)"""
```

Example case (`cases/compression.py`):

```python
import bench

@bench.case(runs=5, tags=["load", "compression"])
def upload_gzip(ctx: bench.Ctx):
    """Load github events to filesystem destination with gzip."""
    pipeline = ctx.pipeline(destination="filesystem")
    pipeline.run(ctx.data("github.events"))
```

## 4. Runner — `bench/runner.py` + `bench/probe.py`

```python
# runner.py

def run(
    case: str | CaseSpec,
    runs: int | None = None,          # override spec
    save: str | None = None,          # label -> store.save() after run
) -> RunResult:
    """Execute a case.

    Per repetition (isolate=True):
      1. mk fresh result_dir and tmp_dir
      2. spawn: sys.executable -m tests.benchmarks.bench.runner --worker
                <module>:<case_name> --run-index <i> --tmp-dir ...
         with env: DLT_BENCH_DIR=<result_dir>, BENCH_PROBE_NET=0/1
      3. parent measures: wall (perf_counter around wait); samples child RSS
         via psutil at ~50ms until exit; cpu from psutil cpu_times() at exit
      4. read all {pid}.jsonl from result_dir, tag events with run=i
      5. read worker sidecar '_worker.json' (see _worker_main)
    Warmup runs execute identically but discard results.
    isolate=False: everything in-process via capture(); wall/rss best-effort.

    Raises CaseFailed (with captured stderr) if any repetition exits non-zero —
    a benchmark that errors must never produce numbers.
    """

def _worker_main(argv: list[str]) -> None:
    """Entry inside the child process.

    - imports case module, builds Ctx
    - if BENCH_PROBE_NET: installs socket counting shim BEFORE case import
    - runs fn(ctx) once
    - writes '_worker.json' sidecar: {
        'wall': <perf_counter around fn only, excludes import cost>,
        'net_bytes_sent'/'net_bytes_recv': from shim,
        'trace': pipeline.last_trace.asdict() if a ctx.pipeline() was created
                 and ran, else None,
        'gc_collections': per-generation deltas,
      }
    - bench events flush via the emitter's own atexit
    Note: parent-measured wall includes interpreter startup; worker-measured
    wall is the reported one. Parent's goes into meta for sanity checks.
    """
```

```python
# probe.py

class RssSampler:
    """Background thread in the parent sampling child RSS via psutil.

    start(pid) / stop() -> peak bytes. Sampling interval 50ms. macOS: only
    memory_info().rss — psutil io_counters() (disk I/O) is unavailable there;
    run macro benchmarks in a Linux container when disk counters matter."""

class SocketShim:
    """In-worker byte accounting. install() monkeypatches socket.socket
    send/recv/sendall (and ssl wrap) with counting wrappers.
    totals() -> (bytes_sent, bytes_recv). Only active when BENCH_PROBE_NET=1;
    never installed in the parent."""
```

## 5. Inline capture — `bench/capture.py`

```python
class Capture:
    events: EventSet          # populated on __exit__
    wall: float
    def __enter__(self) -> "Capture": ...
    def __exit__(self, *exc) -> bool: ...

def capture() -> Capture:
    """Exploratory in-process collection:

        with bench.capture() as cap:
            pipeline.run(...)
        cap.events['file_upload'].sum('file_size')

    Force-enables the emitter in the current process (pointing DLT_BENCH_DIR
    into a tmp dir), snapshots/flushes buffers on exit, merges any worker
    files that appeared. Single run, no stats, no isolation.

    Caveat: normalize's process pool only inherits DLT_BENCH_DIR if the pool
    spawns after capture() starts — a warm pool won't emit. Acceptable for an
    exploration tool; documented, not fixed.
    """
```

## 6. Persistence — `bench/store.py`

```python
RESULTS_DIR = Path("tests/benchmarks/.results")   # gitignored

def save(result: RunResult, label: str) -> Path:
    """Write to .results/{label}/{case_name}/:
        events.jsonl     — all events, run-tagged
        summary.json     — counters(), wall/cpu/rss Stats (WITH raw samples), meta
        traces.json      — per-run pipeline traces
    Overwrites the case dir within the label; other cases untouched.
    Raw per-run samples are mandatory in summary.json — significance testing
    in compare needs them, medians alone are not enough."""

def load(label: str, case_name: str | None = None) -> dict[str, "SavedResult"]:
    """Load one or all cases for a label. SavedResult mirrors RunResult
    (events, counters, stats with raw samples) reconstructed from disk —
    enough for compare() and EventSet queries, no live objects."""

def list_saved() -> dict[str, list[str]]:
    """{label: [case_names]} for CLI 'bench list --saved'."""
```

## 7. Comparison — `bench/compare.py`

```python
class MetricDiff(NamedTuple):
    key: str                  # 'wall' | 'file_upload.sum(file_size)' | ...
    base: Stat | float
    new: Stat | float
    rel_change: float         # (new - base) / base, medians for Stats
    kind: Literal["timing", "counter"]
    verdict: Literal["significant", "not_significant",
                     "changed", "unchanged", "new", "removed"]
    # timing -> significant/not_significant via Welch's t-test on samples
    #           (p < 0.05) AND a practical threshold (|Δ| >= 2%) to avoid
    #           flagging statistically-significant noise
    # counter -> changed/unchanged (exact); new/removed if key on one side only

class ComparisonResult:
    case_name: str
    diffs: list[MetricDiff]
    def table(self, min_change: float = 0.0) -> str:
        """Aligned text table; timing rows first, then counters; humanized
        units; verdict column. Example:

        upload_gzip                devel      mybranch     Δ
          wall                     3.42s      2.87s      -16.1%  ✓ significant
          peak_rss                 412MB      405MB       -1.7%    n.s.
          file_upload.count        24         12         -50%     CHANGED
          file_upload.sum(size)    118.7MB    61.2MB     -48.4%   CHANGED
        """
    def regressions(self, threshold: float = 0.05) -> list[MetricDiff]:
        """Significant timing increases or counter growth beyond threshold —
        usable as an assertion for a future CI gate."""

def compare(
    base: str | RunResult,      # saved label or live result
    new: str | RunResult,
    cases: Sequence[str] | None = None,   # default: intersection of saved cases
) -> list[ComparisonResult]: ...
```

Step durations from pipeline traces (`RunResult.step_durations()`) are diffed
as first-class timing rows (`extract`, `normalize`, `load`).

## 8. CLI — `bench/cli.py`

```python
def main(argv: list[str] | None = None) -> int:
    """
    bench list [--saved]                     # registered cases / saved labels
    bench run <pattern> [--runs N] [--save LABEL] [--keep]
    bench compare <base_label> <new_label> [pattern] [--min-change 0.02]
                  [--fail-on-regression]
    bench show <label> [case]                # print saved summary tables

    <pattern> globs case names and tags ('compression', 'tag:load', '*').
    Exit codes: run -> nonzero on CaseFailed; compare -> nonzero on
    regression only with --fail-on-regression.
    """
```

Invocation: `uv run python -m tests.benchmarks.bench.cli` (or a Makefile
target). No `[project.scripts]` entry — dev-only tool, keep it out of the
shipped package.

Typical workflow:

```bash
# on devel worktree
uv run python -m tests.benchmarks.bench.cli run upload_gzip --save devel
# on feature worktree
uv run python -m tests.benchmarks.bench.cli run upload_gzip --save mybranch
uv run python -m tests.benchmarks.bench.cli compare devel mybranch
```

## 9. Initial instrumentation points in dlt

File/job granularity, safe to keep permanently:

| Event | Where | Fields |
|---|---|---|
| `file_upload` | filesystem destination job / upload path | `file_size`, `table`, `compressed`, `duration` (via `span`) |
| `writer_rotate` | `dlt/common/data_writers/buffered.py` on file close | `file_size`, `items_count`, `table` |
| `normalize_file` | `dlt/normalize/worker.py` per input file | `items_count`, `duration`, `table` |
| `load_job` | `dlt/load/load.py` on job completion | `table`, `state`, `retry_count`, `duration` |
| `schema_evolution` | schema update application | `table`, `new_columns` |

## Implementation order

1. `dlt/common/runtime/bench.py` + unit tests (enable/disable, dump format,
   counters, span)
2. `model.py` (EventSet/Stat — pure, easy to test)
3. `runner.py` worker protocol + minimal `ctx.py` (tmp_dir, pipeline, data)
   → first end-to-end run
4. `registry.py`, `store.py`, `compare.py`
5. `probe.py` net shim, `capture.py`, `cli.py`
6. Instrumentation points + `cases/compression.py` reference case

## Open decisions

- **Field-name conventions** are load-bearing for `counters()` auto-derivation —
  freeze the list (§1) before writing instrumentation.
- **`capture()` warm-pool caveat** (§5): documented, not fixed.
- **Trace diffing** (§7): step durations are included as timing rows — decided
  yes (cheap, high-value).
- **Benchmark environment**: macOS has no per-process disk I/O counters and no
  `pyperf system tune`-level isolation — multiple isolated runs + significance
  testing compensate; use a Linux container when disk/network counters must be
  exact.
