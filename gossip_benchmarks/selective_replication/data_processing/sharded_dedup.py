"""Seven natural Newsgroups sources feeding a fused duplicate-detection reducer.

This is a controlled application-pattern workload, not a production trace.
Partition sizes follow the data; no sleeps, padded outputs or invented costs.
"""

from collections import defaultdict
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import random
import re


SOURCES = ("alt", "comp", "misc", "rec", "sci", "soc", "talk")
WORKLOADS = ("news_dedup_progress", "news_dedup_reduce")


def graph():
    return [("signatures_" + source, []) for source in SOURCES] + [
        ("duplicate_report", list(range(len(SOURCES))))]


def prepare(directory, inputs):
    from benchmark import atomic_json
    from real_workloads import sha_file
    keys = ["dedup_" + source for source in SOURCES]
    if all(key in inputs for key in keys):
        return inputs
    with gzip.open(inputs["news"]["path"], "rt", encoding="utf-8") as stream:
        records = json.load(stream)
    known = set(SOURCES)
    if any(r["category"].split(".")[0] not in known for r in records):
        raise RuntimeError("Unexpected Newsgroups source hierarchy")
    for source, key in zip(SOURCES, keys):
        rows = [r for r in records if r["category"].split(".")[0] == source]
        if not rows:
            raise RuntimeError("No documents for source %s; increase --news-documents" % source)
        path = Path(directory) / (key + ".json.gz")
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(rows, stream)
        inputs[key] = {"path": str(path), "sha256": sha_file(path), "rows": len(rows),
                       "source_prefix": source, "text_characters": sum(len(r["text"]) for r in rows)}
    atomic_json(Path(directory) / "inputs.json", inputs)
    return inputs


def shingles(text, limit):
    tokens = re.findall(r"[a-z]{2,}", text.lower())[:limit]
    return {tuple(tokens[j:j + 3]) for j in range(max(0, len(tokens) - 2))}


def compute(stage, parents, inputs, settings):
    limit = settings["news_tokens"]
    if stage < len(SOURCES):
        with gzip.open(inputs["dedup_" + SOURCES[stage]]["path"], "rt", encoding="utf-8") as stream:
            records = json.load(stream)
        prime = (1 << 61) - 1
        rng = random.Random(1729)
        coefficients = [(rng.randrange(1, prime), rng.randrange(prime)) for _ in range(64)]
        result = []
        for record in records:
            values = {int.from_bytes(hashlib.blake2b(" ".join(s).encode(), digest_size=8).digest(), "big") % prime
                      for s in shingles(record["text"], limit)}
            if values:
                result.append((record["id"], tuple(min((a * x + b) % prime for x in values)
                                                   for a, b in coefficients)))
        return result
    # A fused reduce task: build LSH candidates and verify actual word shingles.
    # Map outputs are the only optional checkpoints; the final report is mandatory.
    signatures = sorted(itertools.chain.from_iterable(parents))
    buckets = defaultdict(list)
    for ident, signature in signatures:
        for band in range(16):
            buckets[band, signature[band * 4:band * 4 + 4]].append(ident)
    candidates = set()
    for ids in buckets.values():
        candidates.update(itertools.combinations(ids, 2))
    needed = {ident for pair in candidates for ident in pair}
    with gzip.open(inputs["news"]["path"], "rt", encoding="utf-8") as stream:
        records = json.load(stream)
    sets = {r["id"]: shingles(r["text"], limit) for r in records if r["id"] in needed}
    verified = []
    for a, b in sorted(candidates):
        intersection = len(sets[a] & sets[b])
        union = len(sets[a] | sets[b])
        if union and 5 * intersection >= 4 * union:
            verified.append([a, b, round(intersection / union, 9)])
    return {"documents_with_shingles": len(signatures), "candidate_pairs": len(candidates),
            "jaccard_threshold": .8, "verified_pairs": verified,
            "signature_sha256": hashlib.sha256(json.dumps(signatures, separators=(",", ":")).encode()).hexdigest()}
