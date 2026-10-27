"""Real public data and explicitly defined analytics/indexing pipelines.

Downloads and execution occur only when the owner invokes the benchmark.
These are application-pattern implementations, not production system replicas.
"""

from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import urllib.request

import sharded_dedup


TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_WORKLOADS = ("taxi_dashboard", "taxi_zone_join", "news_index")
WORKLOADS = DEFAULT_WORKLOADS + sharded_dedup.WORKLOADS


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, path, local=None):
    if path.exists():
        return
    tmp = path.with_suffix(path.suffix + ".partial")
    if local:
        shutil.copyfile(local, tmp)
    else:
        with urllib.request.urlopen(url, timeout=30) as source, tmp.open("wb") as target:
            shutil.copyfileobj(source, target)
    tmp.replace(path)


def prepare(directory, settings):
    from benchmark import atomic_json
    directory.mkdir(exist_ok=True)
    metadata_path = directory / "inputs.json"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text())
        for info in metadata.values():
            if sha_file(info["path"]) != info["sha256"]:
                raise RuntimeError("Input changed since preparation: " + info["path"])
        return metadata
    metadata = {}
    if any(w.startswith("taxi_") for w in settings["workloads"]):
        import duckdb
        import pyarrow.parquet as pq
        raw = directory / "yellow-2024-01.parquet"
        zones = directory / "taxi-zones.csv"
        download(TAXI_URL, raw, settings.get("taxi_file"))
        download(ZONE_URL, zones, settings.get("zones_file"))
        con = duckdb.connect()
        con.execute("SET threads=1")
        con.execute("SET memory_limit='2GB'")
        table = con.execute("""
            SELECT tpep_pickup_datetime AS pickup, tpep_dropoff_datetime AS dropoff,
                   CAST(PULocationID AS INTEGER) AS pu, CAST(DOLocationID AS INTEGER) AS dest,
                   CAST(payment_type AS INTEGER) AS payment,
                   CAST(round(fare_amount * 100) AS BIGINT) AS fare,
                   CAST(round(tip_amount * 100) AS BIGINT) AS tip,
                   CAST(round(total_amount * 100) AS BIGINT) AS total,
                   CAST(round(trip_distance * 1000) AS BIGINT) AS distance
            FROM read_parquet(?)
            WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01'
              AND tpep_pickup_datetime < TIMESTAMP '2024-02-01'
              AND tpep_dropoff_datetime > tpep_pickup_datetime
              AND trip_distance > 0 AND trip_distance < 200
              AND fare_amount > 0 AND fare_amount < 1000
              AND tip_amount >= 0 AND total_amount > 0
              AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL
              AND payment_type IS NOT NULL
            ORDER BY ALL LIMIT ?
        """, [str(raw), settings["taxi_rows"]]).fetch_arrow_table().combine_chunks()
        con.close()
        if table.num_rows == 0:
            raise RuntimeError("Taxi input contains no valid January 2024 records")
        sample = directory / "taxi-sample.parquet"
        pq.write_table(table, sample)
        metadata["taxi"] = {"path": str(sample), "sha256": sha_file(sample), "rows": table.num_rows,
                            "source_url": TAXI_URL, "raw_sha256": sha_file(raw),
                            "sample": "first records by deterministic column ordering after filters"}
        metadata["zones"] = {"path": str(zones), "sha256": sha_file(zones), "source_url": ZONE_URL}
    if any(w.startswith("news_") for w in settings["workloads"]):
        from sklearn.datasets import fetch_20newsgroups
        data = fetch_20newsgroups(data_home=str(directory / "sklearn-cache"), subset="all",
                                 remove=("headers", "footers", "quotes"),
                                 shuffle=True, random_state=settings["seed"])
        records = [{"id": i, "text": text, "category": data.target_names[int(label)]}
                   for i, (text, label) in enumerate(zip(data.data, data.target)) if text.strip()]
        records = records[:settings["news_documents"]]
        path = directory / "news.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(records, stream)
        metadata["news"] = {"path": str(path), "sha256": sha_file(path), "rows": len(records),
                            "source": "20 Newsgroups via sklearn; headers/footers/quotes removed"}
    atomic_json(metadata_path, metadata)
    return metadata


def graph(workload):
    if workload in sharded_dedup.WORKLOADS:
        return sharded_dedup.graph()
    if workload == "taxi_dashboard":
        return [
            ("clean_trips", []), ("trip_features", [0]),
            ("hourly_demand", [1]), ("route_totals", [1]), ("payment_totals", [1]),
            ("demand_trend", [2]), ("route_ranking", [3]),
            ("fare_outliers", [1]), ("outlier_summary", [7]),
            ("daily_totals", [2]), ("report", [4, 5, 6, 8, 9])]
    if workload == "taxi_zone_join":
        return [("trips", []), ("zones", []), ("enriched_trips", [0, 1]),
                ("borough_totals", [2]), ("corridor_totals", [2]),
                ("airport_totals", [2]), ("corridor_ranking", [4]),
                ("borough_trend", [3]), ("borough_payments", [2]), ("report", [5, 6, 7, 8])]
    if workload == "news_index":
        return [("documents", []), ("normalized_text", [0]), ("tokens", [1]),
                ("term_frequencies", [2]), ("inverted_index", [3]), ("minhash", [2]),
                ("duplicate_buckets", [5]), ("corpus_frequencies", [3]),
                ("tfidf_index", [3, 4]), ("search_results", [8]), ("report", [6, 7, 9])]
    raise ValueError(workload)


def context(workload):
    if workload.startswith("taxi_"):
        import duckdb
        import pyarrow  # Initialize before task profiling, equally for every policy.
        con = duckdb.connect()
        con.execute("SET threads=1")
        con.execute("SET memory_limit='2GB'")
        return con
    return None


def canonical(value):
    if hasattr(value, "to_pylist"):
        value = value.to_pylist()
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sql(con, parents, query, params=None):
    for i, parent in enumerate(parents):
        con.register("p%d" % i, parent)
    try:
        return con.execute(query + " ORDER BY ALL", params or []).fetch_arrow_table().combine_chunks()
    finally:
        for i in range(len(parents)):
            con.unregister("p%d" % i)


def compute(workload, stage, parents, inputs, settings, con):
    if workload in sharded_dedup.WORKLOADS:
        return sharded_dedup.compute(stage, parents, inputs, settings)
    specs = graph(workload)
    if stage == len(specs) - 1:
        return {specs[parent][0]: {"sha256": fingerprint(value),
                                  "rows": value.num_rows if hasattr(value, "num_rows") else len(value)}
                for parent, value in zip(specs[stage][1], parents)}
    if workload == "taxi_dashboard":
        queries = [
            "SELECT * FROM read_parquet(?)",
            "SELECT *, date_trunc('hour', pickup) AS hour, CAST(pickup AS DATE) AS day, "
            "date_diff('second', pickup, dropoff) AS duration FROM p0",
            "SELECT hour, day, pu, count(*) AS trips, sum(total) AS revenue, sum(tip) AS tips "
            "FROM p0 GROUP BY hour, day, pu",
            "SELECT pu, dest, count(*) AS trips, sum(duration) AS seconds, sum(total) AS revenue "
            "FROM p0 GROUP BY pu, dest",
            "SELECT payment, count(*) AS trips, sum(fare) AS fares, sum(tip) AS tips FROM p0 GROUP BY payment",
            "SELECT *, sum(trips) OVER (PARTITION BY pu ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) "
            "AS rolling_trips FROM p0",
            "SELECT *, row_number() OVER (PARTITION BY pu ORDER BY trips DESC, dest) AS rank FROM p0",
            "SELECT * FROM p0 WHERE fare > 15000 OR (distance < 1000 AND fare > 5000)",
            "SELECT pu, count(*) AS outliers, sum(fare) AS fares FROM p0 GROUP BY pu",
            "SELECT day, sum(trips) AS trips, sum(revenue) AS revenue FROM p0 GROUP BY day"]
        return sql(con, parents, queries[stage], [inputs["taxi"]["path"]] if stage == 0 else None)
    if workload == "taxi_zone_join":
        queries = [
            "SELECT * FROM read_parquet(?)",
            'SELECT CAST(LocationID AS INTEGER) AS location, Borough AS borough, Zone AS zone FROM read_csv_auto(?)',
            "SELECT t.*, a.borough AS origin_borough, b.borough AS dest_borough, "
            "date_trunc('hour', pickup) AS hour FROM p0 t "
            "JOIN p1 a ON t.pu=a.location JOIN p1 b ON t.dest=b.location",
            "SELECT hour, origin_borough, count(*) AS trips, sum(total) AS revenue "
            "FROM p0 GROUP BY hour, origin_borough",
            "SELECT origin_borough, dest_borough, pu, dest, count(*) AS trips, sum(total) AS revenue "
            "FROM p0 GROUP BY origin_borough, dest_borough, pu, dest",
            "SELECT pu, dest, count(*) AS trips, sum(total) AS revenue FROM p0 "
            "WHERE pu IN (1,132,138) OR dest IN (1,132,138) GROUP BY pu, dest",
            "SELECT *, row_number() OVER (PARTITION BY origin_borough ORDER BY trips DESC, pu, dest) "
            "AS rank FROM p0",
            "SELECT *, sum(trips) OVER (PARTITION BY origin_borough ORDER BY hour "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_trips FROM p0",
            "SELECT origin_borough, payment, count(*) AS trips, sum(fare) AS fares "
            "FROM p0 GROUP BY origin_borough, payment"]
        params = [inputs["taxi"]["path"]] if stage == 0 else [inputs["zones"]["path"]] if stage == 1 else None
        return sql(con, parents, queries[stage], params)
    if stage == 0:
        with gzip.open(inputs["news"]["path"], "rt", encoding="utf-8") as stream:
            return json.load(stream)
    if stage == 1:
        return [(r["id"], " ".join(re.findall(r"[a-z]{2,}", r["text"].lower()))) for r in parents[0]]
    if stage == 2:
        return [(ident, text.split()[:settings["news_tokens"]]) for ident, text in parents[0]]
    if stage == 3:
        return [(ident, dict(sorted(Counter(tokens).items()))) for ident, tokens in parents[0]]
    if stage == 4:
        index = defaultdict(list)
        for ident, counts in parents[0]:
            for token, count in counts.items():
                index[token].append((ident, count))
        return dict(sorted(index.items()))
    if stage == 5:
        signatures = []
        prime = (1 << 61) - 1
        for ident, tokens in parents[0]:
            shingles = {int.from_bytes(hashlib.blake2b(" ".join(tokens[j:j + 3]).encode(),
                         digest_size=8).digest(), "big") % prime for j in range(max(0, len(tokens) - 2))}
            signature = tuple(min((((2 * i + 3) * x + (i + 1) * 104729) % prime
                                    for x in shingles), default=0) for i in range(32))
            signatures.append((ident, signature))
        return signatures
    if stage == 6:
        buckets = defaultdict(list)
        for ident, signature in parents[0]:
            for band in range(8):
                key = str(band) + ":" + ",".join(map(str, signature[band * 4:band * 4 + 4]))
                buckets[key].append(ident)
        return {key: value for key, value in sorted(buckets.items()) if len(value) > 1}
    if stage == 7:
        counts = Counter()
        for _, terms in parents[0]:
            counts.update(terms)
        return dict(sorted(counts.items()))
    if stage == 8:
        frequencies, inverted = parents
        n = len(frequencies)
        weighted = defaultdict(list)
        for ident, terms in frequencies:
            for token, count in terms.items():
                score = (1 + math.log(count)) * (1 + math.log((n + 1) / (len(inverted[token]) + 1)))
                weighted[token].append((ident, round(score, 9)))
        return dict(sorted(weighted.items()))
    if stage == 9:
        answers = {}
        for query in ("computer graphics", "space science", "car engine", "medical treatment"):
            scores = Counter()
            for token in query.split():
                for ident, score in parents[0].get(token, []):
                    scores[ident] += score
            answers[query] = sorted(scores.items(), key=lambda p: (-p[1], p[0]))[:20]
        return answers
    raise ValueError(stage)
