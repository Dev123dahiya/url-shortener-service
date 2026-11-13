import asyncio
import json

import pytest

from shortener import AliasTaken, InvalidUrl, NotFound, Shortener
from shortener.app import create_app


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def service(tmp_path, clock):
    return Shortener({f"s{i}": str(tmp_path / f"s{i}.db") for i in range(3)}, clock=clock, flush_every=10)


def test_shorten_and_resolve(service):
    code = service.shorten("https://example.com/a?b=1")
    assert 6 <= len(code) <= 11
    assert service.resolve(code) == "https://example.com/a?b=1"


@pytest.mark.parametrize("bad", ["", "example.com", "ftp://example.com", "javascript:alert(1)", "http://", "https://e.com/" + "x" * 2100])
def test_rejects_unsafe_or_malformed_urls(service, bad):
    with pytest.raises(InvalidUrl):
        service.shorten(bad)


def test_custom_alias_rules(service):
    assert service.shorten("https://example.com", alias="my-link") == "my-link"
    with pytest.raises(AliasTaken):
        service.shorten("https://other.example", alias="my-link")
    for bad in ["ab", "has space", "stats", "x" * 40]:
        with pytest.raises(InvalidUrl):
            service.shorten("https://example.com", alias=bad)


def test_links_expire(service, clock):
    code = service.shorten("https://example.com", ttl_seconds=60)
    assert service.resolve(code)
    clock.now += 61
    with pytest.raises(NotFound):
        service.resolve(code)


def test_cache_absorbs_repeat_reads_and_negative_lookups(service):
    code = service.shorten("https://example.com")
    for _ in range(50):
        service.resolve(code)
    assert service.cache.misses == 1 and service.cache.hits == 49
    for _ in range(5):
        with pytest.raises(NotFound):
            service.resolve("nope1234")
    assert service.cache.misses == 2  # the miss was cached
    # A cached "not found" must not hide a link created afterwards.
    service.shorten("https://example.com/new", alias="nope1234")
    assert service.resolve("nope1234") == "https://example.com/new"


def test_click_analytics_are_batched_and_grouped(service, clock):
    code = service.shorten("https://example.com")
    for _ in range(7):
        service.resolve(code, referrer="https://news.ycombinator.com/item?id=1")
    for _ in range(3):
        service.resolve(code)
    clock.now += 86_400
    for _ in range(4):
        service.resolve(code, referrer="https://twitter.com/x")
    stats = service.stats(code)
    assert stats["clicks"] == 14
    assert list(stats["by_day"].values()) == [10, 4]
    assert stats["top_referrers"] == {"news.ycombinator.com": 7, "twitter.com": 4, "direct": 3}


def test_links_spread_across_shards_and_stay_findable(service):
    codes = [service.shorten(f"https://example.com/{i}") for i in range(600)]
    counts = service.shard_counts()
    assert sum(counts.values()) == 600 and all(c > 100 for c in counts.values())
    assert all(service.resolve(c) == f"https://example.com/{i}" for i, c in enumerate(codes))


def call(app, method, path, body=None, headers=()):
    sent = []

    async def run():
        async def receive():
            return {"type": "http.request", "body": json.dumps(body).encode() if body is not None else b""}

        async def send(message):
            sent.append(message)

        await app({"type": "http", "method": method, "path": path, "headers": list(headers)}, receive, send)

    asyncio.run(run())
    payload = sent[1]["body"]
    return sent[0]["status"], dict(sent[0]["headers"]), json.loads(payload) if payload else None


def test_http_api_end_to_end(service):
    app = create_app(service, base_url="https://sho.rt")
    status, _, created = call(app, "POST", "/shorten", {"url": "https://example.com/page"})
    assert status == 201 and created["short_url"] == f"https://sho.rt/{created['code']}"

    status, headers, _ = call(app, "GET", f"/{created['code']}", headers=[(b"referer", b"https://github.com/")])
    assert status == 302 and headers[b"location"] == b"https://example.com/page"

    status, _, stats = call(app, "GET", f"/stats/{created['code']}")
    assert status == 200 and stats["clicks"] == 1 and stats["top_referrers"] == {"github.com": 1}

    assert call(app, "POST", "/shorten", {"url": "not a url"})[0] == 400
    assert call(app, "POST", "/shorten", {})[0] == 400
    call(app, "POST", "/shorten", {"url": "https://a.example", "alias": "taken"})
    assert call(app, "POST", "/shorten", {"url": "https://b.example", "alias": "taken"})[0] == 409
    assert call(app, "GET", "/missing00")[0] == 404
    assert call(app, "GET", "/health")[2]["status"] == "ok"
