"""Writes the TPC-BiH benchmark data (see datafinder_examples_tests/src/datafinder_examples_tests/tpch_bih_generator.py).

``uv run datafinder_examples/tools/generate_tpch_bih_benchmark_data.py [--scale 0.001] [--steps 1000] [--seed 1] [--out DIR]``
writes ``tpch_bih_benchmark_<table>.csv``. The tests generate the same data themselves, so these files are only for looking at.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "datafinder_examples_tests" / "src"))

from datafinder_examples_tests.tpch_bih_generator import generate  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=float, default=0.001, help="the TPC-H scale factor h")
    parser.add_argument("--steps", type=int, default=1000, help="the number of scenarios the history runs (the report's m)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", type=Path, default=Path("."), help="where to write the CSV files")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for table, count in generate(args.scale, args.steps, args.seed).write(args.out).items():
        print(f"wrote {args.out / ('tpch_bih_benchmark_' + table + '.csv')} ({count} rows)")
