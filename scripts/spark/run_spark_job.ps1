param(
  [Parameter(Mandatory=$true)] [string]$Dataset
)

$ErrorActionPreference = 'Stop'

Write-Host "[Spark] $Dataset"
python scripts/spark/indegree_distribution.py --dataset $Dataset
if ($LASTEXITCODE -ne 0) {
  Write-Warning "PySpark failed (exit $LASTEXITCODE). Running local fallback to mimic Spark outputs."
  $in = "data/raw/$Dataset.txt"
  $out = "results/spark/$Dataset"
  python scripts/spark/local_spark_fallback.py --input $in --out $out
}
