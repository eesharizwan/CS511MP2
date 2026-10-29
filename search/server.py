#!/usr/bin/env python3
"""CS 511 Project 2, Part 2: the search REST API (runs on main).

    python3 server.py [--port 8000] [--database /data/search.db]

API (the tests rely on it exactly):

    GET /search?q=<query>   ->  200, application/json
        {"query": "<query>",
         "results": [{"id": 155765, "title": "Champaign, Illinois", "score": 1.2e-05}, ...]}
    GET /health             ->  200 once the server can answer queries

Search: split the query into terms with tokenize() (the Project 1 rules) and
keep the distinct terms. An article matches when its text contains every term.
Its score is

    score = PageRank(article) * sum over the terms t of tf(t, article) * ln(N / df(t))

(tf, df and N as in indexer.py). Return the 10 best matches, ordered by
descending score and then ascending id; a query without terms or without
matches returns an empty list.

Only SearchIndex (marked TODO) needs to be written; the HTTP handling is
given. Use any database you like (see serve.sh).
"""
import argparse
import heapq
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import sqlite3

TOP_K = 10

# Tokenization rules of Project 1.
TOKEN_PATTERN = re.compile(r"[a-z]+(?:'[a-z]+)?")
MIN_TOKEN_LENGTH = 2
STOPWORDS = frozenset(
    "a an and are as at be by for from in is it of on or that the to was with".split()
)


def tokenize(text):
    """Project 1 tokenizer: lowercase, match TOKEN_PATTERN, drop short words and stopwords."""
    return [word for word in TOKEN_PATTERN.findall(text.lower())
            if len(word) >= MIN_TOKEN_LENGTH and word not in STOPWORDS]


class SearchIndex:
    """Answers queries from the database that serve.sh loaded."""

    def __init__(self, database):
        """Connect to your database; load what should stay in memory.

        `database` is the --database option (useful if your database is a
        file); ignore it otherwise. Called once, when the server starts.
        """
        # TODO: implement.
        # Connect to the SQLite file and allow multiple threads to read it at the same time so the API doesn't crash on concurrent requests
        self.conn = sqlite3.connect(database, check_same_thread=False)
        # raise NotImplementedError("SearchIndex.__init__")

    def search(self, terms):
        """Return the top TOP_K [(score, article_id, title)] for a set of terms.

        Called concurrently from several threads (one per request). An
        article matches when it contains every term; see the module docstring
        for the score. Return [] when there are no terms or no matches.
        """
        # TODO: implement. heapq.nsmallest(TOP_K, ..., key=lambda s: (-s[0], s[1]))
        # keeps the TOP_K best without sorting every match.

        # If the user didn't search for anything, just return an empty list right away
        if not terms:
            return []
            
        # Open a cursor to talk to the database
        cursor = self.conn.cursor()
        
        # Make a string of question marks like '?,?,?' to safely put our search terms into the SQL query
        placeholders = ','.join(['?'] * len(terms))
        
        # Write the SQL query to join the tables, filter by our terms, group by article, and calculate the final score
        # We sort in SQL (ORDER BY) and grab the top 10 (LIMIT) so we don't need to manually use heapq in Python
        sql = f"""
            SELECT a.article_id, a.title, (a.pagerank * SUM(p.weight)) as final_score
            FROM articles a
            JOIN postings p ON a.article_id = p.article_id
            WHERE p.term IN ({placeholders})
            GROUP BY a.article_id, a.title, a.pagerank
            HAVING COUNT(DISTINCT p.term) = ?
            ORDER BY final_score DESC, a.article_id ASC
            LIMIT 10
        """
        
        # Combine the search terms and the total count of unique terms into one list to pass into the SQL placeholders
        params = list(terms) + [len(terms)]
        
        # Run the big SQL query with our parameters
        cursor.execute(sql, params)
        
        # Loop through the database results and format them exactly as (score, id, title) like the docstring asked
        return [(row[2], row[0], row[1]) for row in cursor.fetchall()]
        # raise NotImplementedError("SearchIndex.search")


# --- Given code -----------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    index = None

    def send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self.send_json(200, {"status": "ok"})
        if url.path != "/search":
            return self.send_json(404, {"error": "not found"})
        query = parse_qs(url.query).get("q", [""])[0]
        results = self.index.search(frozenset(tokenize(query)))
        self.send_json(200, {"query": query, "results": [
            {"id": article_id, "title": title, "score": score}
            for score, article_id, title in results]})

    def log_message(self, *args):
        pass  # keep the log quiet


def main(argv):
    parser = argparse.ArgumentParser(description="Search REST API")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", default="/data/search.db",
                        help="where serve.sh put the index (if your database uses a file)")
    args = parser.parse_args(argv[1:])
    Handler.index = SearchIndex(args.database)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print("serving on port %d" % args.port, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
