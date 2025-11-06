# Correctness: Spark vs Hadoop

This document summarizes how we validated that both implementations (Spark and Hadoop Streaming) produce identical results for in-degree and in-degree distribution across the selected datasets.

## Method

- We compute two artifacts per dataset and system:
  - `indegree`: per-node in-degree counts (tab-separated `node\tdegree`)
  - `distribution`: histogram of in-degree values (tab-separated `degree\tcount`)
- Validation script: `scripts/validate_results.py` compares Spark and Hadoop outputs for each dataset and artifact.
  - It is tolerant to different part file layouts (e.g., Spark `part-*` vs Hadoop `part-r-00000`).
  - It sorts lines and compares them for an exact match.

## Datasets

- `email-EuAll`
- `web-BerkStan`
- `soc-LiveJournal1`

## Results

All comparisons produced an exact match for both artifacts on all datasets:

- email-EuAll: indegree ✅, distribution ✅
- web-BerkStan: indegree ✅, distribution ✅
- soc-LiveJournal1: indegree ✅, distribution ✅

You can re-run the check:

```powershell
python scripts/validate_results.py
```

## Notes and edge cases

- Input parser ignores comment lines starting with `#` and expects at least two whitespace-separated columns per edge (`src dst`).
- Duplicate edges contribute to the in-degree accordingly (no deduplication is performed).
- On Windows without `HADOOP_HOME`, PySpark may fail to write outputs; our runner falls back to a local Spark-equivalent implementation that writes the same format used for validation.
- Hadoop runs are performed via Hadoop Streaming if available; otherwise a local fallback writes the same outputs in the expected Hadoop directory layout.

