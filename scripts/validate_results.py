from pathlib import Path
from collections import Counter

# Prefer container mount (/data/results) if present; otherwise use local ./results
_CONTAINER_RESULTS = Path("/data/results")
_LOCAL_RESULTS = Path("results")
_ROOT = _CONTAINER_RESULTS if _CONTAINER_RESULTS.exists() else _LOCAL_RESULTS

RESULTS = {
    "hadoop": _ROOT / "hadoop",
    "spark": _ROOT / "spark",
}

DATASETS = [
    "email-EuAll",
    "web-BerkStan",
    "soc-LiveJournal1",
]

JOBS = ["distribution", "indegree"]


def _read_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return [ln.strip() for ln in f if ln.strip()]


def read_hadoop_output(dataset: str, job: str) -> Counter:
    base = RESULTS["hadoop"] / dataset / job
    # Support either a single part-r-00000 (streaming/local) or multiple part-* files
    parts = []
    if (base / "part-r-00000").exists():
        parts = [base / "part-r-00000"]
    else:
        parts = [p for p in base.glob("part-*") if p.is_file()]
    if not parts:
        return Counter()
    cnt = Counter()
    for p in parts:
        for s in _read_lines(p):
            cnt[s] += 1
    return cnt


def read_spark_output(dataset: str, job: str) -> Counter:
    base = RESULTS["spark"] / dataset / job
    if not base.exists():
        return Counter()
    parts = [p for p in base.glob("part-*") if p.is_file() and not p.name.endswith(".crc")]
    if not parts:
        return Counter()
    cnt = Counter()
    for p in parts:
        for s in _read_lines(p):
            cnt[s] += 1
    return cnt


def compare(dataset: str, job: str):
    h = read_hadoop_output(dataset, job)
    s = read_spark_output(dataset, job)
    if not h and not s:
        return {
            "status": "missing",
            "message": "Both outputs missing",
        }
    if not h:
        return {"status": "missing", "message": "Hadoop output missing"}
    if not s:
        return {"status": "missing", "message": "Spark output missing"}

    if h == s:
        return {"status": "match", "message": "Exact match"}

    # Differences
    diff_h = h - s  # items more in Hadoop
    diff_s = s - h  # items more in Spark
    extra_h_count = sum(diff_h.values())
    extra_s_count = sum(diff_s.values())

    sample_h = list(diff_h.items())[:5]
    sample_s = list(diff_s.items())[:5]

    return {
        "status": "mismatch",
        "message": f"Diffs: +Hadoop={extra_h_count}, +Spark={extra_s_count}",
        "extra_h_samples": sample_h,
        "extra_s_samples": sample_s,
    }


def main():
    print("Result correctness comparison (Hadoop vs Spark)\n")
    any_mismatch = False
    for dataset in DATASETS:
        print(f"Dataset: {dataset}")
        for job in JOBS:
            res = compare(dataset, job)
            status = res["status"]
            msg = res["message"]
            print(f"  - {job}: {status} - {msg}")
            if status == "mismatch":
                any_mismatch = True
                if res.get("extra_h_samples"):
                    print("    e.g., only in Hadoop:")
                    for k, v in res["extra_h_samples"]:
                        print(f"      {v}x {k}")
                if res.get("extra_s_samples"):
                    print("    e.g., only in Spark:")
                    for k, v in res["extra_s_samples"]:
                        print(f"      {v}x {k}")
        print()

    if any_mismatch:
        exit(2)


if __name__ == "__main__":
    main()
