# Graph Degree

This project computes node in-degree on large graphs using both Hadoop MapReduce and Spark.

## Structure

- `data/`
  - `raw/`: raw SNAP datasets
  - `cleaned/`: cleaned edge lists ("src dst")
  - `sample/`: optional smaller samples
- `scripts/`: helper scripts (fetch, clean, run)
- `hadoop/`: MapReduce implementation (Maven)
- `spark/`: PySpark and optional Scala versions
- `results/`: outputs and plots
- `docker-compose.yml`: optional cluster orchestration
- `Dockerfile`: optional single image build

This repository contains placeholders only. Fill in scripts and code as needed.

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
