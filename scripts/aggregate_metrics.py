#!/usr/bin/env python3
"""
Aggregate performance metrics for Spark and Hadoop runs.

Inputs (created by scripts/with_metrics.sh):
- /data/metrics/spark/<dataset>/job.time
- /data/metrics/spark/<dataset>/job.dstat.csv (optional)
- /data/metrics/hadoop/<dataset>/indegree.time, distribution.time
- /data/metrics/hadoop/<dataset>/*.dstat.csv (optional)

Output:
- /data/metrics/summary.csv with columns:
  framework,dataset,phase,elapsed_seconds,max_rss_kb,cpu_user_s,cpu_sys_s,avg_cpu_util,avg_mem_used_mb,avg_dsk_read_kbps,avg_dsk_writ_kbps,avg_net_recv_kbps,avg_net_send_kbps

Notes:
- If GNU time -v not present, only elapsed_seconds may be filled (others empty)
- Dstat metrics are averages over the run duration (if present)
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, asdict
import os
from pathlib import Path
from typing import Dict, Optional, Tuple, List

# Allow overriding the data root to support running outside containers.
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data"))
METRICS_ROOT = DATA_ROOT / "metrics"
OUT_CSV = METRICS_ROOT / "summary.csv"

TIME_ELAPSED_PATTERNS = [
    re.compile(r"^Elapsed \(wall clock\) time .*: (.+)$"),
    re.compile(r"^elapsed_seconds:\s*(\d+(?:\.\d+)?)$"),
    re.compile(r"^real\s*(\d+\.\d+)$"),
]
TIME_USER = re.compile(r"^User time \(seconds\):\s*(\d+(?:\.\d+)?)$")
TIME_SYS = re.compile(r"^System time \(seconds\):\s*(\d+(?:\.\d+)?)$")
TIME_MAXRSS = re.compile(r"^Maximum resident set size \(kbytes\):\s*(\d+)$")


def parse_hms(s: str) -> float:
    """Parse h:mm:ss or m:ss to seconds."""
    s = s.strip()
    parts = s.split(":")
    parts = [p.strip() for p in parts]
    if len(parts) == 3:
        h, m, sec = parts
        return int(h) * 3600 + int(m) * 60 + float(sec)
    if len(parts) == 2:
        m, sec = parts
        return int(m) * 60 + float(sec)
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_time_file(p: Path) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[int]]:
    elapsed = None
    user = None
    sys = None
    maxrss = None
    if not p.exists():
        return elapsed, user, sys, maxrss
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        m = TIME_USER.match(line)
        if m:
            user = float(m.group(1))
            continue
        m = TIME_SYS.match(line)
        if m:
            sys = float(m.group(1))
            continue
        m = TIME_MAXRSS.match(line)
        if m:
            maxrss = int(m.group(1))
            continue
        for pat in TIME_ELAPSED_PATTERNS:
            m = pat.match(line)
            if m:
                elapsed = parse_hms(m.group(1))
                break
    return elapsed, user, sys, maxrss


@dataclass
class DstatAverages:
    avg_cpu_util: Optional[float] = None
    avg_mem_used_mb: Optional[float] = None
    avg_dsk_read_kbps: Optional[float] = None
    avg_dsk_writ_kbps: Optional[float] = None
    avg_net_recv_kbps: Optional[float] = None
    avg_net_send_kbps: Optional[float] = None


def parse_dstat_csv(p: Path) -> DstatAverages:
    """Parse dstat CSV (0.7.x/0.8.x) robustly.

    The CSV typically contains a banner, metadata rows, one or more header blocks,
    and then data rows. We detect the header row that starts with 'time' and
    aggregate following rows until the next header block or EOF.
    """
    if not p.exists():
        return DstatAverages()

    with p.open("r", encoding="utf-8", errors="ignore") as f:
        rows: List[List[str]] = list(csv.reader(f))

    def is_header_row(r: List[str]) -> bool:
        return len(r) > 0 and r[0].strip().lower() == "time"

    def make_finder(headers: List[str]):
        def find_col(candidates: List[str]) -> Optional[int]:
            lc = [h.strip().lower() for h in headers]
            for i, h in enumerate(lc):
                for cand in candidates:
                    if cand in h:
                        return i
            return None
        return find_col

    def safe_float(x: str) -> Optional[float]:
        try:
            x = x.strip()
            if x == "":
                return None
            return float(x)
        except Exception:
            return None

    # Aggregates
    samples = 0
    sum_usr = sum_sys = sum_idl = 0.0
    sum_mem_used = 0.0
    sum_dread = sum_dwrit = 0.0
    sum_nrecv = sum_nsend = 0.0

    current_headers: Optional[List[str]] = None
    col_cpu_usr = col_cpu_sys = col_cpu_idl = None
    col_mem_used = col_dsk_read = col_dsk_writ = None
    col_net_recv = col_net_send = None

    for r in rows:
        if not r:
            continue
        # Detect header blocks
        if is_header_row(r):
            current_headers = r
            find_col = make_finder(current_headers)
            col_cpu_usr = find_col(["usr"])  # type: ignore[assignment]
            col_cpu_sys = find_col(["sys"])  # type: ignore[assignment]
            col_cpu_idl = find_col(["idl"])  # type: ignore[assignment]
            col_mem_used = find_col(["used"])  # bytes
            # dstat uses various labels; match loosely
            col_dsk_read = find_col(["io/total read", "dsk/total read", "read"])
            col_dsk_writ = find_col(["io/total writ", "dsk/total writ", "writ"])
            col_net_recv = find_col(["net/total recv", "recv"])
            col_net_send = find_col(["net/total send", "send"])
            continue

        # Skip until we've seen a header
        if current_headers is None:
            continue

        # If a row length doesn't match the header, skip
        if len(r) < len(current_headers):
            continue

        # Data row
        samples += 1
        if col_cpu_usr is not None and col_cpu_usr < len(r):
            v = safe_float(r[col_cpu_usr])
            if v is not None:
                sum_usr += v
        if col_cpu_sys is not None and col_cpu_sys < len(r):
            v = safe_float(r[col_cpu_sys])
            if v is not None:
                sum_sys += v
        if col_cpu_idl is not None and col_cpu_idl < len(r):
            v = safe_float(r[col_cpu_idl])
            if v is not None:
                sum_idl += v
        if col_mem_used is not None and col_mem_used < len(r):
            v = safe_float(r[col_mem_used])
            if v is not None:
                sum_mem_used += v
        if col_dsk_read is not None and col_dsk_read < len(r):
            v = safe_float(r[col_dsk_read])
            if v is not None:
                sum_dread += v
        if col_dsk_writ is not None and col_dsk_writ < len(r):
            v = safe_float(r[col_dsk_writ])
            if v is not None:
                sum_dwrit += v
        if col_net_recv is not None and col_net_recv < len(r):
            v = safe_float(r[col_net_recv])
            if v is not None:
                sum_nrecv += v
        if col_net_send is not None and col_net_send < len(r):
            v = safe_float(r[col_net_send])
            if v is not None:
                sum_nsend += v

    if samples == 0:
        return DstatAverages()

    avg_cpu_util: Optional[float] = None
    if sum_idl > 0.0 and samples > 0:
        avg_cpu_util = 100.0 - (sum_idl / samples)
    elif (sum_usr + sum_sys) > 0.0 and samples > 0:
        avg_cpu_util = (sum_usr + sum_sys) / samples

    # dstat 'used' is bytes; convert to MiB
    avg_mem_used_mb: Optional[float] = None
    if sum_mem_used > 0.0 and samples > 0:
        avg_mem_used_mb = (sum_mem_used / samples) / (1024.0 * 1024.0)

    return DstatAverages(
        avg_cpu_util=avg_cpu_util,
        avg_mem_used_mb=avg_mem_used_mb,
        avg_dsk_read_kbps=(sum_dread / samples) if sum_dread else None,
        avg_dsk_writ_kbps=(sum_dwrit / samples) if sum_dwrit else None,
        avg_net_recv_kbps=(sum_nrecv / samples) if sum_nrecv else None,
        avg_net_send_kbps=(sum_nsend / samples) if sum_nsend else None,
    )


# --- sar fallback parsers ---

def _read_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="ignore").splitlines()


def parse_sar_cpu(p: Path) -> Optional[float]:
    lines = _read_lines(p)
    if not lines:
        return None
    # Prefer the Average: summary row
    for line in lines[::-1]:  # reverse scan
        if line.startswith("Average:"):
            parts = line.split()
            # Expected: Average: all %user %nice %system %iowait %steal %idle
            if len(parts) >= 8:
                try:
                    idle = float(parts[-1])
                    return 100.0 - idle
                except Exception:
                    break
            break
    # Fallback: average %idle from data rows (skip header lines containing '%')
    values: List[float] = []
    for line in lines:
        if '%' in line or line.strip() == '' or line.startswith('Linux') or line.startswith('Average:'):
            continue
        parts = line.split()
        if len(parts) >= 8:
            try:
                idle = float(parts[-1])
                values.append(100.0 - idle)
            except Exception:
                continue
    return sum(values) / len(values) if values else None


def parse_sar_mem(p: Path) -> Optional[float]:
    lines = _read_lines(p)
    if not lines:
        return None
    # Prefer Average: row; columns include kbmemfree kbmemused %memused ...
    for line in lines[::-1]:
        if line.startswith("Average:"):
            parts = line.split()
            if len(parts) >= 4:
                try:
                    kbmemused = float(parts[3])
                    return kbmemused / 1024.0
                except Exception:
                    break
            break
    # Fallback: average kbmemused from data rows without '%'
    values: List[float] = []
    for line in lines:
        if '%' in line or line.strip() == '' or line.startswith('Linux') or line.startswith('Average:'):
            continue
        parts = line.split()
        if len(parts) >= 4:
            try:
                kbmemused = float(parts[3])
                values.append(kbmemused / 1024.0)
            except Exception:
                continue
    return sum(values) / len(values) if values else None


def parse_sar_disk(p: Path) -> Tuple[Optional[float], Optional[float]]:
    lines = _read_lines(p)
    if not lines:
        return None, None
    # Prefer Average: bread/s bwrtn/s
    for line in lines[::-1]:
        if line.startswith("Average:"):
            parts = line.split()
            # Expected: Average: tps rtps wtps bread/s bwrtn/s
            if len(parts) >= 6:
                try:
                    read_kbps = float(parts[-2])
                    write_kbps = float(parts[-1])
                    return read_kbps, write_kbps
                except Exception:
                    break
            break
    # Fallback: average last two columns for data rows
    rvals: List[float] = []
    wvals: List[float] = []
    for line in lines:
        if '%' in line or line.strip() == '' or line.startswith('Linux') or line.startswith('Average:'):
            continue
        parts = line.split()
        if len(parts) >= 6:
            try:
                rvals.append(float(parts[-2]))
                wvals.append(float(parts[-1]))
            except Exception:
                continue
    r = sum(rvals) / len(rvals) if rvals else None
    w = sum(wvals) / len(wvals) if wvals else None
    return r, w


def parse_sar_net(p: Path) -> Tuple[Optional[float], Optional[float]]:
    lines = _read_lines(p)
    if not lines:
        return None, None
    # Prefer Average: per-device rows; sum rxkB/s and txkB/s across non-lo devices
    total_rx = 0.0
    total_tx = 0.0
    any_dev = False
    for line in lines:
        if not line.startswith("Average:"):
            continue
        parts = line.split()
        # Expected: Average: IFACE rxpck/s txpck/s rxkB/s txkB/s ...
        if len(parts) >= 6:
            iface = parts[1]
            if iface.lower() == 'iface' or iface == 'lo':
                continue
            try:
                rxkb = float(parts[4])
                txkb = float(parts[5])
                total_rx += rxkb
                total_tx += txkb
                any_dev = True
            except Exception:
                continue
    if any_dev:
        return total_rx, total_tx

    # Fallback: group by timestamp and sum per sample
    samples: Dict[str, Tuple[float, float]] = {}
    for line in lines:
        if 'IFACE' in line or line.strip() == '' or line.startswith('Linux') or line.startswith('Average:'):
            continue
        parts = line.split()
        # e.g., 12:00:01 AM eth0 0.00 0.01 0.00 0.00
        if len(parts) >= 6:
            timekey = parts[0]
            iface = parts[1]
            if iface == 'lo':
                continue
            try:
                rxkb = float(parts[4])
                txkb = float(parts[5])
            except Exception:
                continue
            rx, tx = samples.get(timekey, (0.0, 0.0))
            samples[timekey] = (rx + rxkb, tx + txkb)
    if samples:
        avg_rx = sum(v[0] for v in samples.values()) / len(samples)
        avg_tx = sum(v[1] for v in samples.values()) / len(samples)
        return avg_rx, avg_tx
    return None, None


def collect_records() -> Dict[Tuple[str, str, str], Dict[str, Optional[float] | str]]:
    # Value dict contains both numeric metrics and identifying strings
    records: Dict[Tuple[str, str, str], Dict[str, Optional[float] | str]] = {}

    # Spark
    for ds_dir in sorted((METRICS_ROOT / "spark").glob("*/")):
        dataset = ds_dir.name.rstrip("/")
        key = ("spark", dataset, "job")
        t_elapsed, t_user, t_sys, t_rss = parse_time_file(ds_dir / "job.time")
        dstat = parse_dstat_csv(ds_dir / "job.dstat.csv")
        # sar fallback if dstat missing/empty
        if (
            dstat.avg_cpu_util is None
            and dstat.avg_mem_used_mb is None
            and dstat.avg_dsk_read_kbps is None
            and dstat.avg_dsk_writ_kbps is None
            and dstat.avg_net_recv_kbps is None
            and dstat.avg_net_send_kbps is None
        ):
            cpu = parse_sar_cpu(ds_dir / "job.sar.cpu.txt")
            mem = parse_sar_mem(ds_dir / "job.sar.mem.txt")
            dread, dwrit = parse_sar_disk(ds_dir / "job.sar.dsk.txt")
            nrecv, nsend = parse_sar_net(ds_dir / "job.sar.net.txt")
            dstat = DstatAverages(
                avg_cpu_util=cpu,
                avg_mem_used_mb=mem,
                avg_dsk_read_kbps=dread,
                avg_dsk_writ_kbps=dwrit,
                avg_net_recv_kbps=nrecv,
                avg_net_send_kbps=nsend,
            )
        records[key] = {
            "framework": "spark",
            "dataset": dataset,
            "phase": "job",
            "elapsed_seconds": t_elapsed,
            "max_rss_kb": float(t_rss) if t_rss is not None else None,
            "cpu_user_s": t_user,
            "cpu_sys_s": t_sys,
            **asdict(dstat),
        }

    # Hadoop
    for ds_dir in sorted((METRICS_ROOT / "hadoop").glob("*/")):
        dataset = ds_dir.name.rstrip("/")
        for phase in ("indegree", "distribution"):
            key = ("hadoop", dataset, phase)
            t_elapsed, t_user, t_sys, t_rss = parse_time_file(ds_dir / f"{phase}.time")
            dstat = parse_dstat_csv(ds_dir / f"{phase}.dstat.csv")
            records[key] = {
                "framework": "hadoop",
                "dataset": dataset,
                "phase": phase,
                "elapsed_seconds": t_elapsed,
                "max_rss_kb": float(t_rss) if t_rss is not None else None,
                "cpu_user_s": t_user,
                "cpu_sys_s": t_sys,
                **asdict(dstat),
            }

    return records


def _write_summary_to(path: Path, rows: List[Dict[str, Optional[float] | str]]) -> None:
    fields = [
        "framework",
        "dataset",
        "phase",
        "elapsed_seconds",
        "max_rss_kb",
        "cpu_user_s",
        "cpu_sys_s",
        "avg_cpu_util",
        "avg_mem_used_mb",
        "avg_dsk_read_kbps",
        "avg_dsk_writ_kbps",
        "avg_net_recv_kbps",
        "avg_net_send_kbps",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fields})


def write_summary(recs: Dict[Tuple[str, str, str], Dict[str, Optional[float] | str]]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = [row for _, row in sorted(recs.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2]))]
    try:
        _write_summary_to(OUT_CSV, sorted_rows)
        print(f"Wrote {OUT_CSV}")
    except PermissionError:
        alt = OUT_CSV.with_name("summary.alt.csv")
        _write_summary_to(alt, sorted_rows)
        print(f"Permission denied writing {OUT_CSV}. Wrote fallback {alt}")


def main() -> int:
    recs = collect_records()
    write_summary(recs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
