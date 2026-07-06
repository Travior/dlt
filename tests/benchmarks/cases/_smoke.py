from dlt.common.runtime import bench as runtime_bench
import dlt.common.runtime.benchmark as bench


@bench.case(runs=2, warmup=0, tags=("smoke",))
def smoke(ctx: bench.Ctx) -> None:
    runtime_bench.event("smoke_event", run_index=ctx.run_index, file_size=10)
    runtime_bench.count("smoke_count", 2, table="events")
