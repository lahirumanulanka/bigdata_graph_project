param(
  [Parameter(Mandatory=$true)] [string]$Dataset,
  [switch]$ForceSpark
)

$ErrorActionPreference = 'Stop'

Write-Host "[Spark] $Dataset"
if (-not $ForceSpark -and -not $env:HADOOP_HOME) {
  Write-Host "HADOOP_HOME not set; skipping PySpark and using local fallback (optimization)."
  $in = "data/raw/$Dataset.txt"
  $out = "results/spark/$Dataset"
  python scripts/spark/local_spark_fallback.py --input $in --out $out
} else {
  python scripts/spark/indegree_distribution.py --dataset $Dataset
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "PySpark failed (exit $LASTEXITCODE). Running local fallback to mimic Spark outputs."
    $in = "data/raw/$Dataset.txt"
    $out = "results/spark/$Dataset"
    python scripts/spark/local_spark_fallback.py --input $in --out $out
  }
}
