# In-degree Distribution on Big Graphs (Spark + Hadoop)

This project computes node in-degree and the in-degree distribution on large real-world graphs using two systems:

- Apache Spark (PySpark)
- Apache Hadoop (MapReduce in Java)

It includes data acquisition, cleaning, batch jobs, experiment runners, basic metrics capture, and plotting.

## What’s included

Project layout (key paths):

- `docker-compose.yml` — Spin up Spark and Hadoop services with correct mounts
- `scripts/`
  - `fetch_snap.sh` — Download SNAP datasets safely; robust to 404/HTML errors
  - `clean_snap.py` — Normalize raw files to simple "src dst" edge lists in `/data/cleaned`
  - `run_spark_experiments.sh` — Run PySpark indegree + distribution on 3 datasets, capture timings
  - `build_hadoop.sh` — Build the MapReduce JAR inside Hadoop NameNode
  - `run_hadoop_experiments.sh` — Run MR indegree + distribution on 3 datasets via HDFS/YARN
  - `plot_distributions.py` — Create linear and log-log plots comparing outputs
- `spark/indegree_spark.py` — PySpark job to compute per-node indegree and degree distribution
- `hadoop/src/main/java/com/example/degree/`
  - `InDegreeMapper.java`, `InDegreeReducer.java`, `InDegreeDriver.java`
  - `DegreeDistMapper.java`, `DegreeDistReducer.java`, `DegreeDistDriver.java`
- `data/` (bind-mounted into containers)
  - `raw/` — raw SNAP downloads (txt)
  - `cleaned/` — normalized edge lists
  - `results/spark/…` — Spark outputs (indegree, distribution)
  - `metrics/` — simple timing outputs for Spark runs
- `results/` (host):
  - `hadoop/…` — Hadoop outputs fetched from HDFS
  - `metrics/` — timing outputs for Hadoop jobs (if permissions allow)
  - `plots/` — if plotting to host; we default to `/data/plots` inside container for fewer permission issues

Web UIs (once containers are up):

- Spark Master UI: http://localhost:8080
- Hadoop NameNode UI: http://localhost:9870
- YARN ResourceManager UI: http://localhost:8088

## Step-by-step: what was done and why

1) Orchestrated the environment with Docker Compose

- Services used: Hadoop NameNode + DataNode + ResourceManager + NodeManager; Spark master + worker.
- Mounted host folders so data and scripts persist and are easy to access:
  - `./data -> /data` in Spark and Hadoop containers (datasets, cleaned edges, results)
  - `./scripts -> /data/scripts` in Spark/Hadoop to run bash/python tools
  - `./spark -> /spark` to run PySpark job files
  - `./hadoop -> /project/hadoop` in NameNode to build the JAR in-place
  - `./results -> /results` available to Hadoop for copying outputs from HDFS; for Spark we prefer `/data/results` for fewer permission issues on Windows

2) Data ingestion and cleaning (SNAP)

- Implemented `scripts/fetch_snap.sh` with safety:
  - Uses `curl -fL --retry 3` (fail on HTTP errors, follow redirects, retry transient errors)
  - Verifies gzip header before `gunzip`; if not gzip (e.g., server returns HTML), it leaves the file as-is and warns
  - Example: `soc-Pokec-relationships.txt.gz` currently returns 404; the script warns and continues
- Implemented `scripts/clean_snap.py` that:
  - Reads only `.txt` files from `/data/raw`
  - Drops comment/empty lines (prefixes: `#`, `%`, `;`)
  - Splits on whitespace, writes `src dst` to `/data/cleaned/<basename>.edges`
  - Prints per-file stats and totals

3) Spark implementation (PySpark)

- `spark/indegree_spark.py` reads edge lists, computes:
  - Per-node indegree: count occurrences of each destination node
  - Degree distribution: group by indegree value and count nodes having that indegree
- Outputs TSV files:
  - `<out>/indegree/part-*` lines are `node\tindegree`
  - `<out>/distribution/part-*` lines are `degree\tcount`
- Practicalities implemented:
  - Filters out comment/empty lines
  - Handles output directory existence (you must `rm -rf` outputs before re-running)
  - Writes under `/data/results/spark/<dataset>` to avoid Windows bind permission issues on `/results`

4) Hadoop implementation (MapReduce, Java)

- Per-node indegree:
  - Mapper `InDegreeMapper` emits `(dst, 1)` for each line `src dst`
  - Reducer `InDegreeReducer` sums to produce `(node, indegree)`
- Degree distribution:
  - Mapper `DegreeDistMapper` reads TSV `(node\tindegree)` and emits `(indegree, 1)`
  - Reducer `DegreeDistReducer` sums counts per indegree value
- Drivers wire jobs with standard `TextInputFormat` and `TextOutputFormat`.
- Built with Java 8 target (`pom.xml` set to `<maven.compiler.release>8</maven.compiler.release>`) to match Hadoop 3.2.1 images.
- Provided `scripts/build_hadoop.sh` to compile with `$(hadoop classpath)` and jar into `target/graph-degree-hadoop.jar` inside the NameNode.

5) Experiment runners + metrics

- Spark: `scripts/run_spark_experiments.sh`
  - PATH sets `/opt/spark/bin` for `spark-submit`
  - Datasets list: `email-EuAll`, `web-BerkStan`, `soc-LiveJournal1`
  - Outputs per dataset under `/data/results/spark/<dataset>`
  - Metrics:
    - Uses `/usr/bin/time -v` when available
    - Else falls back to simple wall-clock `elapsed_seconds` written to `/data/metrics/spark_<dataset>.time`

- Hadoop: `scripts/run_hadoop_experiments.sh`
  - PATH sets `/opt/hadoop-3.2.1/bin` for `hdfs` and `hadoop`
  - Uploads cleaned `.edges` to HDFS under `/exp/in/<dataset>`
  - Runs two jobs per dataset: indegree, then distribution
  - Copies outputs from HDFS to host under `./results/hadoop/<dataset>/...`
  - Captures timing (`/usr/bin/time -v` if present) for each phase to `./results/metrics/`

6) Plotting

- `scripts/plot_distributions.py` merges part files and produces scatter plots on linear and log-log axes:
  - Reads Spark results from `/data/results/spark/<dataset>/distribution/`
  - Reads Hadoop results from `/results/hadoop/<dataset>/distribution/` (if present)
  - Writes PNGs to `/data/plots/<dataset>.png` and `<dataset>_loglog.png`
- Installed `python3-matplotlib` inside `spark-master` using apt for convenience.

## How to run (Windows PowerShell)

Start at project root `C:\Users\ASUS\Documents\bigdata_graph_project`.

1) Start the cluster

```powershell
docker compose up -d
```

2) Fetch and clean datasets (inside spark-master)

```powershell
docker exec -it spark-master bash
# convert Windows CRLF if needed and fetch
sed -i 's/\r$//' /data/scripts/fetch_snap.sh
bash /data/scripts/fetch_snap.sh
# clean to normalized edges
python3 /data/scripts/clean_snap.py
exit
```

3) Run Spark experiments

Option A: all datasets via helper script

```powershell
docker exec -it spark-master bash -lc "sed -i 's/\r$//' /data/scripts/run_spark_experiments.sh; bash /data/scripts/run_spark_experiments.sh"
```

Outputs:

- Per dataset under `data\results\spark\<dataset>\{indegree,distribution}`
- Timings under `data\metrics\spark_<dataset>.time`

Option B: single dataset example

```powershell
docker exec -it spark-master bash -lc "SPARK_HOME=/opt/spark PATH=\"$SPARK_HOME/bin:$PATH\" spark-submit /spark/indegree_spark.py /data/cleaned/web-BerkStan.edges /data/results/spark/web-BerkStan"
```

4) Start YARN services for Hadoop

```powershell
docker compose up -d resourcemanager nodemanager
```

5) Build the Hadoop JAR in NameNode

```powershell
docker exec -it namenode bash -lc "sed -i 's/\r$//' /data/scripts/build_hadoop.sh; bash /data/scripts/build_hadoop.sh"
```

6) Run Hadoop experiments

```powershell
docker exec -it namenode bash -lc "sed -i 's/\r$//' /data/scripts/run_hadoop_experiments.sh; bash /data/scripts/run_hadoop_experiments.sh"
```

Outputs:

- Fetched to host under `results\hadoop\<dataset>\{indegree,distribution}`
- Timings under `results\metrics\hadoop_<dataset>_{indegree,dist}.time`

7) Plot in-degree distributions

```powershell
# install matplotlib (one-time) and plot inside spark-master
docker exec -u 0 -it spark-master bash -lc "apt-get update && apt-get install -y python3-matplotlib && python3 /data/scripts/plot_distributions.py"
```

Plots go to `data\plots\<dataset>.png` and `data\plots\<dataset>_loglog.png`.

## Interpreting and comparing results

1) Correctness of results

- For each dataset, compare Spark vs Hadoop degree distributions:
  - Files: Spark `/data/results/spark/<dataset>/distribution/part-*` vs Hadoop `results/hadoop/<dataset>/distribution/part-*`
  - After plotting, distributions for the same dataset should closely overlay (allowing for ordering/partitioning differences). Any large discrepancies usually signal input differences or cleaning issues.

2) Performance metrics collected

- Spark timings: `data/metrics/spark_<dataset>.time`
  - When `/usr/bin/time -v` exists, includes max RSS (memory), CPU stats, etc.
  - Otherwise contains `elapsed_seconds` captured via a fallback timer.
- Hadoop timings: `results/metrics/hadoop_<dataset>_{indegree,dist}.time`
  - Prefer running on Linux where `/usr/bin/time -v` is available for richer metrics.
- Optional live monitoring:
  - `docker stats` for container CPU/memory
  - Spark UI (http://localhost:8080) and YARN UI (http://localhost:8088)

3) System design and processing approach

- Spark (PySpark): in-memory data processing with RDD transformations. We:
  - Read text -> map to edges -> map (dst,1) -> reduceByKey -> indegree
  - Map (indegree,1) -> reduceByKey -> distribution
  - Suited for iterative and interactive workflows; easy to add more analytics in Python/SQL APIs.
- Hadoop (MapReduce): disk-centric batch processing.
  - Stage 1 (indegree): Map (dst,1) -> Reduce sum
  - Stage 2 (distribution): Map (indegree,1) -> Reduce sum
  - Stable, scalable for large one-off batch jobs; more boilerplate, slower iterations.

## Re-running, extending, and troubleshooting

- Re-running jobs: delete previous outputs first
  - Spark: `rm -rf /data/results/spark/<dataset>`
  - Hadoop (HDFS): `/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /exp/out/...`
- Line endings on Windows: convert scripts before running in Linux containers
  - `sed -i 's/\r$//' /data/scripts/*.sh`
- Missing tools in containers:
  - curl/gzip: `apt-get update && apt-get install -y curl gzip`
  - matplotlib for plotting: `apt-get install -y python3-matplotlib`
- SNAP download hiccups:
  - Some URLs (e.g., Pokec) may be 404 or blocked; the fetch script will warn and continue.
- Permissions on `/results` (Windows bind mounts):
  - We direct Spark outputs to `/data/results/...` to avoid permission errors.
- YARN services show as "unhealthy" initially:
  - They often stabilize; ensure NameNode and DataNode are healthy.

## Datasets used (3+)

- email-EuAll (OK)
- web-BerkStan (OK)
- soc-LiveJournal1 (large; OK; longer runtime)
- soc-Pokec-relationships (currently 404; skipped)

## Notes on scalability

- For large files (hundreds of MBs to many GBs), processing may take minutes to hours depending on hardware and I/O.
- Tuning knobs for Spark:
  - `--conf spark.sql.shuffle.partitions` (we default to 200)
  - Executors/cores/memory if using a multi-worker setup
- For Hadoop: adjust container memory and number of reducers as needed; here we rely on defaults for simplicity.

## License

This project is provided for educational purposes. Datasets are from SNAP; please respect their usage terms.

## Quickstart (Docker)

Prerequisites: Docker Desktop

1) Start the cluster

   - Windows PowerShell
     - `docker compose up -d`

2) Enter the Spark master (optional interactive)

   - `docker exec -it spark-master bash`

3) Fetch and clean SNAP datasets (non-interactive)

   - `docker exec spark-master bash -lc "set -e; sed -i 's/\r$//' /data/scripts/fetch_snap.sh; bash /data/scripts/fetch_snap.sh && python3 /data/scripts/clean_snap.py"`

Outputs:

- Raw downloads: `data/raw/*.txt`
- Cleaned edge lists: `data/cleaned/*.edges` (space-separated `src dst`)

## Monitoring progress

- While fetching, large files (hundreds of MBs+) may take minutes or longer depending on bandwidth.
- You can tail progress by running inside the container: `ls -lh /data/raw` and later `ls -lh /data/cleaned`.

## Troubleshooting

- If `curl` or `gzip` are missing in the container:
  - `docker exec -it spark-master bash -lc "apt-get update && apt-get install -y curl gzip"`
- If `fetch_snap.sh` has Windows CRLF line endings, convert before running:
  - `docker exec spark-master bash -lc "sed -i 's/\r$//' /data/scripts/fetch_snap.sh"`
- Network issues to SNAP can cause partial downloads; rerun the fetch step. `curl -O` will overwrite.

## Notes

- The `./scripts` folder is mounted into containers at `/data/scripts`.
- The `./data` folder is mounted at `/data` to persist downloads and cleaned outputs on the host.
