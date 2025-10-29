#!/usr/bin/env bash
set -euo pipefail

mkdir -p /data/raw
cd /data/raw

# Robust curl options: -f fail on HTTP errors, -L follow redirects, --retry for transient failures, -C - resume
curl_opts=( -fL --retry 3 --retry-delay 5 -C - )

# Corrected Pokec URL (lowercase)
curl "${curl_opts[@]}" -O https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz
curl "${curl_opts[@]}" -O https://snap.stanford.edu/data/email-EuAll.txt.gz
curl "${curl_opts[@]}" -O https://snap.stanford.edu/data/cit-Patents.txt.gz
curl "${curl_opts[@]}" -O https://snap.stanford.edu/data/web-BerkStan.txt.gz
curl "${curl_opts[@]}" -O https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

# Unzip all gz files if present
shopt -s nullglob
gz_files=( *.gz )
if (( ${#gz_files[@]} > 0 )); then
	gunzip -f "${gz_files[@]}"
fi