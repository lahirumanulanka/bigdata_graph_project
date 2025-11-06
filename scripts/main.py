#!/usr/bin/env python3
"""
All-in-one pipeline runner.

Runs the full workflow in one command:
  1) Ensure Python deps
  2) Run experiments with metrics for Spark & Hadoop (with fallbacks)
  3) Validate correctness (Spark vs Hadoop)
  4) Plot distributions
  5) Plot performance metrics per dataset
  6) Compute dataset sizes and plot scaling trends
  7) Run optimized variants and plot before/after comparisons

Usage examples (Windows PowerShell):
  python scripts/main.py                      # run everything (default datasets)
  python scripts/main.py --no-optimized      # skip optimized runs/plots
  python scripts/main.py --steps validate plots  # run a subset of steps
  python scripts/main.py --dry-run           # print what would run
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


DEFAULT_DATASETS = [
    "email-EuAll",
    "web-BerkStan",
    "soc-LiveJournal1",
]


def run(cmd: list[str] | str, dry: bool = False) -> int:
    if isinstance(cmd, str):
        disp = cmd
        args = cmd
    else:
        disp = " ".join(shlex.quote(c) for c in cmd)
        args = cmd
    print(f"$ {disp}")
    if dry:
        return 0
    proc = subprocess.run(args, check=False)
    return proc.returncode


class PipelineRunner:
    def __init__(self, datasets: list[str], optimized: bool, dry_run: bool) -> None:
        self.datasets = datasets
        self.optimized = optimized
        self.dry = dry_run

    def ensure_python_deps(self) -> None:
        print("\n=== Ensure Python dependencies ===")
        if Path("requirements.txt").exists():
            run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], self.dry)
        else:
            run([sys.executable, "-m", "pip", "install", "pyspark", "matplotlib", "pandas", "psutil"], self.dry)

    def run_experiments(self) -> None:
        print("\n=== Run experiments (Spark + Hadoop) with metrics ===")
        # scripts/run_experiments.ps1 already wraps each run with metrics
        # If datasets are provided, pass them; otherwise use default inside the script
        if self.datasets != DEFAULT_DATASETS:
            # Pass as a comma-separated list to -Datasets parameter
            ds_arg = ",".join(self.datasets)
            cmd = [
                "powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
                "scripts/run_experiments.ps1", "-Datasets", ds_arg,
            ]
        else:
            cmd = [
                "powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
                "scripts/run_experiments.ps1",
            ]
        run(cmd, self.dry)

    def validate(self) -> None:
        print("\n=== Validate results correctness (Hadoop vs Spark) ===")
        run([sys.executable, "scripts/validate_results.py"], self.dry)

    def plot_distributions(self) -> None:
        print("\n=== Plot in-degree distributions ===")
        run([sys.executable, "scripts/plot_distributions.py"], self.dry)

    def metrics_plots(self) -> None:
        print("\n=== Plot per-dataset performance metrics ===")
        run([sys.executable, "scripts/plot_metrics.py"], self.dry)

    def dataset_sizes(self) -> None:
        print("\n=== Compute dataset sizes ===")
        run([sys.executable, "scripts/dataset_stats.py"], self.dry)

    def scaling_plots(self) -> None:
        print("\n=== Plot scaling trends ===")
        run([sys.executable, "scripts/plot_scaling.py"], self.dry)

    def optimized_runs(self) -> None:
        if not self.optimized:
            return
        print("\n=== Run optimized variants and collect metrics ===")
        for d in self.datasets:
            # Spark optimized (system label: spark_opt)
            run([
                sys.executable, "scripts/metrics/runner.py",
                "--system", "spark_opt", "--dataset", d, "--",
                "powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
                "scripts/spark/run_spark_job.ps1", "-Dataset", d,
            ], self.dry)
        for d in self.datasets:
            # Hadoop optimized (system label: hadoop_opt)
            run([
                sys.executable, "scripts/metrics/runner.py",
                "--system", "hadoop_opt", "--dataset", d, "--",
                "powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
                "scripts/hadoop/run_hadoop_job.ps1", "-Dataset", d,
            ], self.dry)

    def optimization_plots(self) -> None:
        if not self.optimized:
            return
        print("\n=== Plot before/after optimization comparisons ===")
        run([sys.executable, "scripts/plot_optimizations.py"], self.dry)

    def run_all(self, steps: list[str]) -> None:
        # Step registry
        registry = {
            "deps": self.ensure_python_deps,
            "experiments": self.run_experiments,
            "validate": self.validate,
            "plots-distribution": self.plot_distributions,
            "plots-metrics": self.metrics_plots,
            "sizes": self.dataset_sizes,
            "plots-scaling": self.scaling_plots,
            "optimized-runs": self.optimized_runs,
            "plots-optimizations": self.optimization_plots,
        }

        if not steps or steps == ["all"]:
            order = [
                "deps", "experiments", "validate",
                "plots-distribution", "plots-metrics",
                "sizes", "plots-scaling",
                "optimized-runs", "plots-optimizations",
            ]
        else:
            order = steps

        for key in order:
            fn = registry.get(key)
            if not fn:
                print(f"Unknown step: {key}")
                sys.exit(2)
            fn()
        print("\n=== Pipeline completed ===")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="All-in-one pipeline runner")
    p.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS, help="Datasets to run (default: 3 provided)")
    p.add_argument("--steps", nargs="*", default=["all"], help="Subset of steps to run (default: all)")
    p.add_argument("--no-optimized", dest="optimized", action="store_false", help="Skip optimized runs/plots")
    p.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    runner = PipelineRunner(datasets=args.datasets, optimized=args.optimized, dry_run=args.dry_run)
    runner.run_all(args.steps)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
