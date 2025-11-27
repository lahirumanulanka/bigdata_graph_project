# Hadoop vs. Spark: A Project on In-Degree Distribution

This was a university project where I explored how to calculate the in-degree for nodes in large graphs. I built two versions to compare them:

- One using **Apache Spark** (with PySpark).
- Another using **Apache Hadoop** (with MapReduce Streaming).

Everything is set up to run locally on a Windows machine. If you don't have Hadoop installed, don't worry! I wrote a simple Python script that mimics the Hadoop output, so you can still run everything.

## The Data

The datasets are from the SNAP collection and are already included in the `data/raw/` folder:

- `email-EuAll.txt`
- `web-BerkStan.txt`
- `soc-LiveJournal1.txt`

They are just simple text files where each line represents an edge (`source_node destination_node`). My code is set up to ignore any lines that start with a `#` comment.

## Getting Started (for Windows PowerShell)

Here’s how to get everything running.

**What you'll need:**

- Python 3.8 or newer
- Java 8+ (PySpark needs this to work)
- **Optional:** A proper Hadoop installation. If you don't have it, my fallback script will take over.

**Steps:**

1.  **Install the Python packages:**

    ```
    python -m pip install -r requirements.txt
    ```

2.  **Run all the experiments:**

    This one script runs both the Spark and Hadoop jobs for all three datasets.

    ```
    pwsh scripts/run_experiments.ps1
    ```

    The results will be saved in these folders:
    -   **Spark:** `results/spark/<dataset>/`
    -   **Hadoop:** `results/hadoop/<dataset>/`

3.  **Check the results and make some plots (Optional):**

    I wrote a couple of scripts to validate the outputs and plot the in-degree distributions.

    ```
    python scripts/validate_results.py
    python scripts/plot_distributions.py
    ```

    You can find the plots in the `results/plots` folder.

## Performance Metrics & Plots

While the experiments are running, a script is also measuring how much CPU, memory, disk, and network each process is using. All that data gets saved under `results/metrics/<system>/<dataset>/`.

- `timeseries.csv`: Contains the raw metrics collected every second.
- `summary.json`: Has the final totals, like total time taken.

To see a visual comparison, you can generate plots from this data:

```
python scripts/plot_metrics.py
```

This will create a few comparison charts in `results/metrics/plots/` for each dataset, which is pretty useful for seeing the performance differences.

## Want to Run Jobs Manually?

If you want to run a single job instead of all of them, here’s how.

**Spark** (this example is for the `email-EuAll` dataset):

```
python scripts/spark/indegree_distribution.py --dataset email-EuAll
```

**Hadoop Streaming** (you'll need Hadoop installed for this):

```
# First job: Calculate in-degree for each node
hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar ^
	-D mapreduce.job.reduces=1 ^
	-input data/raw/email-EuAll.txt ^
	-output results/hadoop/email-EuAll/indegree ^
	-mapper "python scripts/hadoop/mapper_in_degree.py" ^
	-reducer "python scripts/hadoop/reducer_in_degree.py"

# Second job: Create a histogram from the in-degrees
hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar ^
	-D mapreduce.job.reduces=1 ^
	-input results/hadoop/email-EuAll/indegree ^
	-output results/hadoop/email-EuAll/distribution ^
	-mapper "python scripts/hadoop/mapper_histogram.py" ^
	-reducer "python scripts/hadoop/reducer_histogram.py"
```

**If you don't have Hadoop**, you can use my fallback script to get the same output:

```
python scripts/hadoop/local_hadoop_fallback.py --input data/raw/email-EuAll.txt --out results/hadoop/email-EuAll
```

## A Few Notes

- The code ignores any lines in the data files that start with `#`.
- Spark runs in "local mode," which is why you need Java installed.
- The Hadoop jobs are also set up for local mode. The PowerShell script tries to find your Hadoop installation automatically.

## Project Documentation

I wrote up my findings in the `docs/` folder. Check them out for a deeper dive!

- `docs/correctness.md` — How I checked if the results were right.
- `docs/performance.md` — My analysis of the performance metrics and plots.
- `docs/system-design.md` — Details on how I built everything and the choices I made.

## The "Just Run Everything" Command

I made a main script that runs the entire pipeline for you, from installing dependencies to running experiments and generating all the plots.

```
python scripts/main.py
```

You can also pass it some options:

- **Choose specific datasets:**
  `python scripts/main.py --datasets email-EuAll web-BerkStan`
- **Skip the optimized runs:**
  `python scripts/main.py --no-optimized`
- **Run only certain steps:**
  `python scripts/main.py --steps deps experiments validate`
- **See what commands it will run without actually running them:**
  `python scripts/main.py --dry-run`

