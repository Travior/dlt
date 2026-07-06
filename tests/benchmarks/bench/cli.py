import argparse
from typing import Optional

from tests.benchmarks.bench.registry import discover
from tests.benchmarks.bench.runner import run
from tests.benchmarks.bench.store import list_saved, load


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="bench")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--saved", action="store_true")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("pattern")
    run_parser.add_argument("--runs", type=int, default=None)
    run_parser.add_argument("--save", default=None)

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("label")
    show_parser.add_argument("case", nargs="?")

    args = parser.parse_args(argv)
    if args.command == "list":
        if args.saved:
            for label, cases in list_saved().items():
                print(f"{label}: {', '.join(cases)}")
        else:
            for spec in discover():
                tags = f" [{', '.join(spec.tags)}]" if spec.tags else ""
                print(f"{spec.name}{tags}")
        return 0
    if args.command == "run":
        specs = discover(args.pattern)
        if not specs:
            raise SystemExit(f"No benchmark cases match {args.pattern!r}")
        for spec in specs:
            print(run(spec, runs=args.runs, save=args.save).table())
        return 0
    if args.command == "show":
        for case_name, saved in load(args.label, args.case).items():
            print(case_name)
            for key, value in saved["summary"]["counters"].items():
                print(f"  {key}: {value}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
