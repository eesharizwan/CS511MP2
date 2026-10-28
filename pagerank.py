#!/usr/bin/env python3
"""CS 511 Project 2, Part 1: PageRank over the Wikipedia link graph.

Rank the articles stored in HDFS by their link structure. PageRank models a
random surfer: with probability DAMPING the surfer follows a random link of the
current page; otherwise (and always on a page without links) the surfer jumps
to a random article. The rank of a page is the long-run fraction of time the
surfer spends on it.

    spark-submit --master spark://main:7077 /pagerank.py \
        <input_uri> <output_uri> [--graph-only] [--damping C]

Both URIs must start with hdfs://main:9000/.

Input: one JSON article per line, as in Project 1, e.g.

    {"id":10,"title":"MapReduce","text":"Map maps data ...","links":[20,30]}

`links` lists the ids of the articles this article links to.

Output directories below <output_uri>:

    graph/        id<TAB>outdeg<TAB>t1,t2,...     (sorted by id)
    ranks/        id<TAB>rank                     (highest rank first, then id)
    convergence/  iteration<TAB>delta             (one line per iteration)

Only the functions marked TODO need to be written. `make pagerank` and the
tests submit this file through scripts/run-pagerank.sh.
"""
import argparse
import json
import sys
from operator import add

from pyspark.sql import SparkSession

DAMPING = 0.85          # c: the surfer follows a link with probability c
EPSILON = 1e-8          # stop once ||R_{i+1} - R_i||_1 <= EPSILON
MAX_ITERATIONS = 100    # ... or after this many iterations
CHECKPOINT_EVERY = 10   # checkpoint the ranks every this many iterations
CHECKPOINT_DIR = "hdfs://main:9000/spark/checkpoints"


# --- The link graph -------------------------------------------------------------

def is_integer(value):
    """True for JSON integers. bool is a subclass of int in Python: exclude it."""
    return isinstance(value, int) and not isinstance(value, bool)


def parse_article(line):
    """Parse one input line into an (article_id, links) pair.

    Return None unless the line is a JSON object whose `id` is an integer.
    A missing or non-list `links` counts as no links, and link entries that
    are not integers are dropped. Keep duplicates, self-links and links to
    unknown articles here: build_graph removes them.
    """
    try:
        obj = json.loads(line)
    except ValueError:
        return None                      # not valid JSON
    if not isinstance(obj, dict):
        return None                      # valid JSON, but not an object
    article_id = obj.get("id")
    if not is_integer(article_id):
        return None                      # missing, string, bool or float id
    links = obj.get("links")
    if not isinstance(links, list):
        links = []                       # missing or non-list links = no links
    return (article_id, [t for t in links if is_integer(t)])


def build_graph(lines):
    """Turn an RDD of raw input lines into an RDD of (article_id, targets).

    Every valid article appears exactly once, including articles without
    out-links (dangling pages), whose targets list is empty. `targets` is the
    ascending list of distinct article ids the article links to, without the
    article itself and without ids that are not valid articles of the input
    (broken links). Article ids are unique.
    """
    # (id, raw integer links) for every valid article.
    articles = lines.map(parse_article).filter(lambda kv: kv is not None)

    # All valid ids, as (id, None), to check link targets against.
    valid_ids = articles.mapValues(lambda _: None)

    # Candidate edges keyed by target: (target, source). Deduplicate and drop
    # self-links per article before the shuffle.
    edges = articles.flatMap(
        lambda kv: [(t, kv[0]) for t in set(kv[1]) if t != kv[0]])

    # Keep only edges whose target is a valid article, re-keyed by source.
    kept = edges.join(valid_ids).map(lambda kv: (kv[1][0], kv[0]))

    # Attach the kept targets to every article; articles with no kept
    # targets get None from the left outer join and become [].
    return articles.leftOuterJoin(kept.groupByKey()).mapValues(
        lambda v: sorted(set(v[1])) if v[1] is not None else [])


# --- PageRank -------------------------------------------------------------------

def contributions(targets, rank, damping):
    """Return the (target, share) pairs a page with `rank` sends along its links.

    The page splits damping * rank evenly over its targets. A dangling page
    (no targets) sends nothing.
    """
    if not targets:
        return []                        # dangling page: its rank goes to everyone
    share = damping * rank / len(targets)
    return [(t, share) for t in targets]


def pagerank_step(links, ranks, num_pages, damping, num_partitions, checkpoint=False):
    """One PageRank iteration.

    `links` and `ranks` are (id, targets) and (id, rank) RDDs with the same
    partitioner. Compute

        R_{i+1}(u) = sum of the contributions u receives
        d          = 1 - ||R_{i+1}||_1        (rank not passed along links)
        R_{i+1}(u) = R_{i+1}(u) + d / num_pages

    and return (R_{i+1}, delta) where delta = ||R_{i+1} - R_i||_1 (the sum of
    absolute differences). The new ranks must contain every page, keep the
    partitioner of `ranks`, and be persisted. When `checkpoint` is true, also
    mark them for checkpointing; Spark only honours checkpoint() if it is
    called before the first action that computes the RDD.
    """
    # 1. Rank each page receives along links: (target, summed shares).
    incoming = links.join(ranks) \
        .flatMap(lambda kv: contributions(kv[1][0], kv[1][1], damping)) \
        .reduceByKey(add, num_partitions) \
        .persist()

    # 3. Rank not passed along links (the other 15% plus dangling pages),
    #    shared equally. One number comes back to the driver.
    passed = incoming.values().sum()
    jump = (1.0 - passed) / num_pages

    # 2. Every page (even with no in-links) gets its incoming sum + jump.
    #    leftOuterJoin from `ranks` keeps all pages; mapValues keeps the
    #    partitioner.
    new_ranks = ranks.leftOuterJoin(incoming).mapValues(
        lambda v: (v[1] if v[1] is not None else 0.0) + jump)

    # 4. Persist and mark for checkpointing before any action on new_ranks.
    new_ranks.persist()
    if checkpoint:
        new_ranks.checkpoint()

    # 5. delta = sum |new - old|. This action materializes new_ranks (and
    #    writes the checkpoint).
    delta = new_ranks.join(ranks) \
        .map(lambda kv: abs(kv[1][0] - kv[1][1])) \
        .sum()

    incoming.unpersist()
    return new_ranks, delta


def run_pagerank(links, num_pages, damping=DAMPING, epsilon=EPSILON,
                 max_iterations=MAX_ITERATIONS, checkpoint_every=CHECKPOINT_EVERY):
    """Iterate pagerank_step from R_0(u) = 1 / num_pages until delta <= epsilon.

    Stop after max_iterations at the latest. Checkpoint the ranks every
    `checkpoint_every` iterations so that the lineage does not grow without
    bound, and unpersist ranks that are no longer needed.
    Return (ranks, deltas), where deltas[i] is the delta of iteration i + 1.
    """
    num_partitions = links.getNumPartitions()
    initial = 1.0 / num_pages
    ranks = links.mapValues(lambda _: initial).persist()

    deltas = []
    for i in range(1, max_iterations + 1):
        new_ranks, delta = pagerank_step(
            links, ranks, num_pages, damping, num_partitions,
            checkpoint=(i % checkpoint_every == 0))
        ranks.unpersist()                # release the previous ranks
        ranks = new_ranks
        deltas.append(delta)
        print("ITERATION %d DELTA %.6e" % (i, delta))
        if delta <= epsilon:
            break
    return ranks, deltas


# --- Given code -----------------------------------------------------------------

def format_graph_record(article_id, targets):
    return "%d\t%d\t%s" % (article_id, len(targets), ",".join(map(str, targets)))


def format_rank_record(article_id, rank):
    return "%d\t%.12e" % (article_id, rank)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("input_uri")
    parser.add_argument("output_uri")
    parser.add_argument("--graph-only", action="store_true",
                        help="stop after writing graph/")
    parser.add_argument("--damping", type=float, default=DAMPING)
    parser.add_argument("--epsilon", type=float, default=EPSILON)
    parser.add_argument("--max-iterations", type=int, default=MAX_ITERATIONS)
    parser.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR)
    return parser.parse_args(argv[1:])


def main(argv):
    args = parse_args(argv)
    output = args.output_uri.rstrip("/")

    spark = SparkSession.builder.appName("PageRank").getOrCreate()
    sc = spark.sparkContext
    sc.setCheckpointDir(args.checkpoint_dir)
    try:
        # Partition the graph once and keep it in memory: every iteration
        # joins against it, and a shared partitioner avoids reshuffling it.
        num_partitions = max(sc.defaultParallelism, 2)
        links = build_graph(sc.textFile(args.input_uri)) \
            .partitionBy(num_partitions) \
            .persist()

        links.sortByKey(numPartitions=1) \
            .map(lambda kv: format_graph_record(kv[0], kv[1])) \
            .saveAsTextFile(output + "/graph")
        num_pages = links.count()
        print("GRAPH NODES: %d" % num_pages)
        print("GRAPH EDGES: %d" % links.values().map(len).sum())
        print("GRAPH DANGLING: %d" % links.filter(lambda kv: not kv[1]).count())
        if args.graph_only:
            return 0

        ranks, deltas = run_pagerank(links, num_pages, args.damping, args.epsilon,
                                     args.max_iterations)
        ranks.sortBy(lambda kv: (-kv[1], kv[0]), numPartitions=1) \
            .map(lambda kv: format_rank_record(kv[0], kv[1])) \
            .saveAsTextFile(output + "/ranks")
        sc.parallelize(["%d\t%.6e" % (i + 1, d) for i, d in enumerate(deltas)], 1) \
            .saveAsTextFile(output + "/convergence")
        print("PAGERANK ITERATIONS: %d" % len(deltas))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
