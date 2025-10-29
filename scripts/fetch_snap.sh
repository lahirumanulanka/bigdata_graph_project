#!/usr/bin/env bash
set -euo pipefail
mkdir -p /data/raw
cd /data/raw

urls=(
	https://snap.stanford.edu/data/soc-Pokec-relationships.txt.gz
	https://snap.stanford.edu/data/email-EuAll.txt.gz
	https://snap.stanford.edu/data/cit-Patents.txt.gz
	https://snap.stanford.edu/data/web-BerkStan.txt.gz
	https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
)

for u in "${urls[@]}"; do
	f=$(basename "$u")
	echo "Downloading $f ..."
	# -f: fail on HTTP errors; -L: follow redirects; --retry: retry transient errors
	if ! curl -fL --retry 3 -o "$f" "$u"; then
		echo "Warning: failed to download $u" >&2
		continue
	fi
	# Test gzip before extracting; skip if not valid gzip (e.g., HTML error page)
	if gzip -t "$f" >/dev/null 2>&1; then
		gunzip -f "$f"
	else
		echo "Warning: $f is not a valid gzip; leaving as-is" >&2
	fi
done
