"""Measure the parts: python -m shortener"""
from __future__ import annotations

import json
import os
import random
import tempfile
import time

from .ring import HashRing
from .service import Shortener


def main() -> None:
    tmp = tempfile.mkdtemp()
    service = Shortener({f"shard-{i}": os.path.join(tmp, f"shard-{i}.db") for i in range(4)}, cache_size=2_000)

    n = 20_000
    start = time.perf_counter()
    codes = [service.shorten(f"https://example.com/articles/{i}") for i in range(n)]
    write_rate = n / (time.perf_counter() - start)

    # Zipf-like traffic: a few links get most of the clicks.
    rng = random.Random(3)
    weights = [1 / (rank + 1) for rank in range(n)]
    lookups = rng.choices(codes, weights=weights, k=100_000)
    start = time.perf_counter()
    for code in lookups:
        service.resolve(code)
    read_rate = len(lookups) / (time.perf_counter() - start)
    service.flush_clicks()

    # How many keys move when a 5th shard joins?
    keys = [f"key-{i}" for i in range(50_000)]
    ring = HashRing([f"shard-{i}" for i in range(4)])
    before = {k: ring.node_for(k) for k in keys}
    ring.add("shard-4")
    moved_ring = sum(before[k] != ring.node_for(k) for k in keys) / len(keys)
    moved_modulo = sum(hash(k) % 4 != hash(k) % 5 for k in keys) / len(keys)

    counts = service.shard_counts()
    print(json.dumps({
        "links_created": n, "unique_codes": len(set(codes)), "code_length": len(codes[0]),
        "shard_distribution": counts,
        "largest_shard_vs_even_split": round(max(counts.values()) / (n / 4), 3),
        "cache": {"capacity": service.cache.capacity, "hit_rate": round(service.cache.hit_rate, 3)},
        "keys_moved_when_adding_5th_shard": {"consistent_hashing": round(moved_ring, 3), "modulo_hashing": round(moved_modulo, 3)},
        "single_thread_throughput_per_second": {"shorten": round(write_rate), "resolve": round(read_rate)},
    }, indent=2))


if __name__ == "__main__":
    main()
