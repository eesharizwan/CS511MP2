#!/bin/bash
# CS 511 Project 2, Part 2: build the index and start the search API on main.
#
#   bash serve.sh <articles_uri> <ranks_uri>
#
# `make serve` and the tests copy this directory to /search on main and run
# this script there (so hdfs, spark-submit and python3 are at hand). It must
# (re)build everything from the given inputs, start the server in the
# background on port 8000, and return once GET /health answers. Run it twice
# and the second run must replace the first server.
set -e
cd "$(dirname "$0")"

ARTICLES_URI="$1"
RANKS_URI="$2"
INDEX_URI="hdfs://main:9000/search/index"
DATABASE=/data/search.db

# 0. Stop a server started earlier.
pkill -f '[s]erver.py' || true

# 1. Build the index with Spark.
hdfs dfs -rm -r -f "$INDEX_URI"
spark-submit --master spark://main:7077 indexer.py "$ARTICLES_URI" "$RANKS_URI" "$INDEX_URI"

# 2. Load it into your database.
# TODO: start your database if it runs as a service (or install it in the
# Dockerfiles and start it in start-main.sh), and load the index into it,
# e.g. hdfs dfs -cat "${INDEX_URI}/postings/part-*" | <your loader>.
#echo
#exit 1
mkdir -p /data
rm -f "$DATABASE"

python3 << 'EOF'
import sqlite3
import subprocess
import ast

# Connect to the SQLite database
conn = sqlite3.connect('/data/search.db')
c = conn.cursor()

# Create tables
c.execute('CREATE TABLE postings (term TEXT, article_id INTEGER, weight REAL)')
c.execute('CREATE TABLE articles (article_id INTEGER PRIMARY KEY, title TEXT, pagerank REAL)')

print("Loading postings into SQLite...")
# Stream the postings from HDFS
postings_proc = subprocess.Popen(['hdfs', 'dfs', '-cat', 'hdfs://main:9000/search/index/postings/part-*'], stdout=subprocess.PIPE)
for line in postings_proc.stdout:
    line_str = line.decode('utf-8').strip()
    if not line_str:
        continue
        
    # Split the term from the list of postings using the tab character
    parts = line_str.split('\t', 1)
    if len(parts) != 2:
        continue
    term = parts[0]
    postings_raw = parts[1]
    
    # Split the comma-separated list, then split each id:weight pair
    for pair in postings_raw.split(','):
        if ':' in pair:
            article_id, weight = pair.split(':', 1)
            c.execute('INSERT INTO postings (term, article_id, weight) VALUES (?, ?, ?)',
                      (term, int(article_id), float(weight)))

print("Loading articles into SQLite...")
# Stream the articles/docs from HDFS
docs_proc = subprocess.Popen(['hdfs', 'dfs', '-cat', 'hdfs://main:9000/search/index/docs/part-*'], stdout=subprocess.PIPE)
for line in docs_proc.stdout:
    line_str = line.decode('utf-8').strip()
    if not line_str:
        continue
        
    # Split by tab into exactly 3 parts: id, rank, title
    parts = line_str.split('\t', 2)
    if len(parts) == 3:
        article_id = int(parts[0])
        rank = float(parts[1])
        title = parts[2]
        c.execute('INSERT INTO articles (article_id, title, pagerank) VALUES (?, ?, ?)',
                  (article_id, title, rank))
    else:
        print(f"Skipping malformed doc line: {line_str}")

print("Creating database indexes for fast querying...")
c.execute('CREATE INDEX idx_term ON postings(term)')
c.execute('CREATE INDEX idx_article ON postings(article_id)')

conn.commit()
conn.close()
EOF

# 3. Start the server in the background and wait until it answers.
setsid nohup python3 server.py --port 8000 > /search/server.log 2>&1 < /dev/null &
for _ in $(seq 1 120); do
    if python3 -c 'import urllib.request; urllib.request.urlopen("http://localhost:8000/health", timeout=2)' \
            2>/dev/null; then
        echo "search API ready on port 8000"
        exit 0
    fi
    sleep 1
done
echo "the server did not start; see /search/server.log" >&2
exit 1
