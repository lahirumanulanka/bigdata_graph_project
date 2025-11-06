param(
  [Parameter(Mandatory=$true)] [string]$Dataset
)

$ErrorActionPreference = 'Stop'

function Get-HadoopStreamingJarPath {
  if ($env:HADOOP_HOME) {
    $cand = Get-ChildItem -Path (Join-Path $env:HADOOP_HOME 'share/hadoop/tools/lib') -Filter 'hadoop-streaming-*.jar' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($cand) { return $cand.FullName }
    $cand = Get-ChildItem -Path (Join-Path $env:HADOOP_HOME 'share/hadoop/tools') -Filter 'hadoop-streaming-*.jar' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($cand) { return $cand.FullName }
  }
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

Write-Host "[Hadoop] $Dataset"
$input = "data/raw/$Dataset.txt"
$outRoot = "results/hadoop/$Dataset"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null

$streamJar = Get-HadoopStreamingJarPath
$hasHadoop = (Get-Command hadoop -ErrorAction SilentlyContinue) -ne $null -and $streamJar -ne $null

if ($hasHadoop) {
  Write-Host "Using Hadoop Streaming jar: $streamJar"
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "$outRoot/indegree"
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "$outRoot/distribution"

  hadoop jar $streamJar `
    -D mapreduce.job.reduces=1 `
    -input $input `
    -output "$outRoot/indegree" `
    -mapper "python scripts/hadoop/mapper_in_degree.py" `
    -reducer "python scripts/hadoop/reducer_in_degree.py" `
    -combiner "python scripts/hadoop/reducer_in_degree.py"

  hadoop jar $streamJar `
    -D mapreduce.job.reduces=1 `
    -input "$outRoot/indegree" `
    -output "$outRoot/distribution" `
    -mapper "python scripts/hadoop/mapper_histogram.py" `
    -reducer "python scripts/hadoop/reducer_histogram.py" `
    -combiner "python scripts/hadoop/reducer_histogram.py"
} else {
  Write-Warning "Hadoop or Streaming JAR not found. Running local fallback to mimic Hadoop outputs."
  python scripts/hadoop/local_hadoop_fallback.py --input $input --out $outRoot
}
