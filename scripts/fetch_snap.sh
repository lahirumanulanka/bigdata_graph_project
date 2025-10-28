#!/usr/bin/env bash
set -e
mkdir -p /data/raw
cd /data/raw
curl -O https://snap.stanford.edu/data/soc-Pokec-relationships.txt.gz
curl -O https://snap.stanford.edu/data/email-EuAll.txt.gz
curl -O https://snap.stanford.edu/data/cit-Patents.txt.gz
curl -O https://snap.stanford.edu/data/web-BerkStan.txt.gz
curl -O https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
gunzip -f *.gz