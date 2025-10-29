#!/usr/bin/env bash
set -euo pipefail

# Build Hadoop MapReduce JAR inside the namenode container.
# Expects project mounted at /project/hadoop

cd /project/hadoop

echo "[build] Cleaning target/"
rm -rf target
mkdir -p target/classes

HADOOP_BIN=${HADOOP_BIN:-/opt/hadoop-3.2.1/bin/hadoop}
if ! command -v hadoop >/dev/null 2>&1; then
	echo "[build] hadoop not in PATH, using $HADOOP_BIN"
else
	HADOOP_BIN=$(command -v hadoop)
fi
CP=$($HADOOP_BIN classpath)
echo "[build] Using Hadoop classpath of length ${#CP}"

echo "[build] Compiling Java sources"
javac -cp "$CP" -d target/classes $(find src/main/java -name "*.java")

echo "[build] Creating JAR"
jar -cvf target/graph-degree-hadoop.jar -C target/classes . >/dev/null

echo "[build] Done: target/graph-degree-hadoop.jar"
