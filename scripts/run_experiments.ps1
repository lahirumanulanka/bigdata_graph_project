param(
  [string[]]$Datasets = @('email-EuAll','web-BerkStan','soc-LiveJournal1')
)

$ErrorActionPreference = 'Stop'

function Ensure-PythonDeps {
  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python is required on PATH. Install Python 3.8+ and retry."
  }
  if (-not (Get-Command pip -ErrorAction SilentlyContinue)) {
    Write-Warning "pip not found on PATH; attempting 'python -m pip'"
  }
  Write-Host "Installing Python dependencies (pyspark, matplotlib) if needed..."
  python -m pip install --upgrade pip | Out-Null
  if (Test-Path "requirements.txt") {
    python -m pip install -r requirements.txt
  } else {
    python -m pip install pyspark matplotlib
  }
}

function Run-SparkJob([string]$dataset) {
  Write-Host "[Spark] $dataset (with metrics)"
  python scripts/metrics/runner.py --system spark --dataset $dataset -- `
    powershell.exe -ExecutionPolicy Bypass -File scripts/spark/run_spark_job.ps1 -Dataset $dataset
}

function Get-HadoopStreamingJarPath {
  # Try HADOOP_HOME first
  if ($env:HADOOP_HOME) {
    $cand = Get-ChildItem -Path (Join-Path $env:HADOOP_HOME 'share/hadoop/tools/lib') -Filter 'hadoop-streaming-*.jar' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($cand) { return $cand.FullName }
    $cand = Get-ChildItem -Path (Join-Path $env:HADOOP_HOME 'share/hadoop/tools') -Filter 'hadoop-streaming-*.jar' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($cand) { return $cand.FullName }
  }
  # Try common install locations
  $common = @(
    'C:/hadoop/share/hadoop/tools/lib',
    'C:/Program Files/hadoop/share/hadoop/tools/lib'
  )
  foreach ($p in $common) {
    if (Test-Path $p) {
      $cand = Get-ChildItem -Path $p -Filter 'hadoop-streaming-*.jar' -ErrorAction SilentlyContinue | Select-Object -First 1
      if ($cand) { return $cand.FullName }
    }
  }
  return $null
}

function Run-HadoopJobs([string]$dataset) {
  Write-Host "[Hadoop] $dataset (with metrics)"
  python scripts/metrics/runner.py --system hadoop --dataset $dataset -- `
    powershell.exe -ExecutionPolicy Bypass -File scripts/hadoop/run_hadoop_job.ps1 -Dataset $dataset
}

# Main
Ensure-PythonDeps

foreach ($d in $Datasets) {
  Run-SparkJob -dataset $d
}

foreach ($d in $Datasets) {
  Run-HadoopJobs -dataset $d
}

Write-Host "All jobs submitted. Outputs under results/spark and results/hadoop."
