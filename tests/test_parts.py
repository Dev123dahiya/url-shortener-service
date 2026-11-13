import threading

import pytest

from shortener import HashRing, LRUCache, SnowflakeGenerator, decode_base62, encode_base62


def test_base62_round_trip_and_ordering():
    for n in [0, 1, 61, 62, 3843, 3844, 2**63 - 1]:
        assert decode_base62(encode_base62(n)) == n
    assert encode_base62(61) == "z" and encode_base62(62) == "10"
    with pytest.raises(ValueError):
        decode_base62("abc!")
    with pytest.raises(ValueError):
        encode_base62(-1)


def test_snowflake_ids_are_unique_and_increasing_across_threads():
    gen = SnowflakeGenerator(node_id=7)
    out: list[list[int]] = []

    def worker():
        out.append([gen.next_id() for _ in range(5_000)])

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    flat = [i for chunk in out for i in chunk]
    assert len(set(flat)) == 40_000
    for chunk in out:
        assert chunk == sorted(chunk)
    assert SnowflakeGenerator.parts(flat[0])["node_id"] == 7


def test_snowflake_survives_sequence_overflow_and_clock_going_backwards():
    now = [1_800_000_000_000]
    gen = SnowflakeGenerator(1, clock_ms=lambda: now[0])
    burst = [gen.next_id() for _ in range(5_000)]  # more than 4,096 in one frozen millisecond
    assert len(set(burst)) == 5_000 and burst == sorted(burst)
    now[0] -= 60_000  # clock steps back a minute
    after = gen.next_id()
    assert after > burst[-1]


def test_different_nodes_never_collide():
    now = [1_800_000_000_000]
    a, b = SnowflakeGenerator(1, lambda: now[0]), SnowflakeGenerator(2, lambda: now[0])
    assert not {a.next_id() for _ in range(1000)} & {b.next_id() for _ in range(1000)}
    with pytest.raises(ValueError):
        SnowflakeGenerator(1024)


def test_ring_is_stable_and_balanced():
    ring = HashRing(["a", "b", "c", "d"])
    keys = [f"k{i}" for i in range(20_000)]
    owners = [ring.node_for(k) for k in keys]
    assert owners == [HashRing(["d", "c", "b", "a"]).node_for(k) for k in keys]  # order-independent
    for node in "abcd":
        assert 0.15 < owners.count(node) / len(keys) < 0.35


def test_ring_moves_few_keys_when_membership_changes():
    ring = HashRing(["a", "b", "c", "d"])
    keys = [f"k{i}" for i in range(20_000)]
    before = {k: ring.node_for(k) for k in keys}
    ring.add("e")
    moved = [k for k in keys if ring.node_for(k) != before[k]]
    assert all(ring.node_for(k) == "e" for k in moved)  # keys only move to the new node
    assert 0.10 < len(moved) / len(keys) < 0.30            # about 1/5, not 4/5
    ring.remove("e")
    assert {k: ring.node_for(k) for k in keys} == before
    with pytest.raises(LookupError):
        HashRing().node_for("x")


def test_lru_evicts_least_recently_used():
    cache = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1  # 'a' is now the most recent
    cache.put("c", 3)           # evicts 'b'
    assert cache.get("b") is None and cache.get("a") == 1 and cache.get("c") == 3
    assert cache.evictions == 1 and cache.hits == 3 and cache.misses == 1


def test_lru_ttl_expiry():
    now = [0.0]
    cache = LRUCache(10, ttl=5, clock=lambda: now[0])
    cache.put("k", "v")
    now[0] = 4.9
    assert cache.get("k") == "v"
    now[0] = 5.1
    assert cache.get("k") is None and len(cache) == 0
