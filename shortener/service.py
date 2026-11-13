from __future__ import annotations

import re
import sqlite3
import threading
import time
from typing import Callable
from urllib.parse import urlsplit

from .cache import LRUCache
from .ids import SnowflakeGenerator, encode_base62
from .ring import HashRing

SCHEMA = """
CREATE TABLE IF NOT EXISTS links (
    code       TEXT PRIMARY KEY,
    url        TEXT NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL
);
CREATE TABLE IF NOT EXISTS clicks (
    code     TEXT NOT NULL,
    day      TEXT NOT NULL,       -- YYYY-MM-DD (UTC)
    referrer TEXT NOT NULL,
    count    INTEGER NOT NULL,
    PRIMARY KEY (code, day, referrer)
);
"""
ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")
RESERVED = {"shorten", "stats", "health"}


class InvalidUrl(ValueError):
    pass


class AliasTaken(ValueError):
    pass


class NotFound(LookupError):
    pass


class Shortener:
    """Create and resolve short links across several database shards.

    * Codes come from Snowflake IDs in base62, so no shard needs to ask another for the next ID.
    * The code picks the shard through a consistent-hash ring.
    * Resolves go through an LRU cache; misses are cached too, so repeated lookups of
      a non-existent code do not reach the database.
    * Clicks are buffered in memory and written in batches.
    """

    def __init__(
        self,
        shard_paths: dict[str, str],
        node_id: int = 0,
        cache_size: int = 10_000,
        cache_ttl: float | None = 300.0,
        clock: Callable[[], float] = time.time,
        flush_every: int = 100,
    ) -> None:
        self._clock = clock
        self._ids = SnowflakeGenerator(node_id)
        self.ring = HashRing(sorted(shard_paths))
        self._paths = dict(shard_paths)
        self._local = threading.local()
        self.cache = LRUCache(cache_size, cache_ttl)
        self._pending: dict[tuple[str, str, str], int] = {}
        self._pending_lock = threading.Lock()
        self._pending_total = 0
        self._flush_every = flush_every
        for name in self._paths:
            self._conn(name).executescript(SCHEMA)

    def _conn(self, shard: str) -> sqlite3.Connection:
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = self._local.conns = {}
        if shard not in conns:
            conn = sqlite3.connect(self._paths[shard], timeout=30, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conns[shard] = conn
        return conns[shard]

    # ---- write path ------------------------------------------------------
    @staticmethod
    def validate_url(url: str) -> str:
        url = url.strip()
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname or len(url) > 2048:
            raise InvalidUrl("only absolute http(s) URLs up to 2048 characters are accepted")
        return url

    def shorten(self, url: str, alias: str | None = None, ttl_seconds: float | None = None) -> str:
        url = self.validate_url(url)
        now = self._clock()
        expires = now + ttl_seconds if ttl_seconds else None
        if alias is not None:
            if not ALIAS_RE.match(alias) or alias.lower() in RESERVED:
                raise InvalidUrl("alias must be 3-32 characters of letters, digits, '-' or '_', and not reserved")
            code = alias
        else:
            code = encode_base62(self._ids.next_id())
        shard = self.ring.node_for(code)
        try:
            self._conn(shard).execute(
                "INSERT INTO links (code, url, created_at, expires_at) VALUES (?, ?, ?, ?)", (code, url, now, expires)
            )
        except sqlite3.IntegrityError:
            raise AliasTaken(code) from None
        self.cache.delete(code)  # drop any cached "not found" for this code
        return code

    # ---- read path -------------------------------------------------------
    def resolve(self, code: str, referrer: str = "") -> str:
        """Return the target URL and count the click. Raises NotFound for unknown or expired codes."""
        now = self._clock()
        entry = self.cache.get(code)
        if entry is None:
            row = self._conn(self.ring.node_for(code)).execute(
                "SELECT url, expires_at FROM links WHERE code = ?", (code,)
            ).fetchone()
            entry = (row[0], row[1]) if row else (None, None)
            self.cache.put(code, entry)
        url, expires = entry
        if url is None or (expires is not None and expires <= now):
            raise NotFound(code)
        self._record_click(code, referrer, now)
        return url

    def _record_click(self, code: str, referrer: str, now: float) -> None:
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        host = urlsplit(referrer).hostname or "direct"
        with self._pending_lock:
            key = (code, day, host)
            self._pending[key] = self._pending.get(key, 0) + 1
            self._pending_total += 1
            due = self._pending_total >= self._flush_every
        if due:
            self.flush_clicks()

    def flush_clicks(self) -> int:
        """Write buffered clicks, one transaction per shard. Returns clicks written."""
        with self._pending_lock:
            pending, self._pending, self._pending_total = self._pending, {}, 0
        by_shard: dict[str, list[tuple[str, str, str, int]]] = {}
        for (code, day, host), n in pending.items():
            by_shard.setdefault(self.ring.node_for(code), []).append((code, day, host, n))
        for shard, rows in by_shard.items():
            conn = self._conn(shard)
            conn.execute("BEGIN")
            conn.executemany(
                "INSERT INTO clicks (code, day, referrer, count) VALUES (?, ?, ?, ?)"
                " ON CONFLICT (code, day, referrer) DO UPDATE SET count = count + excluded.count", rows)
            conn.execute("COMMIT")
        return sum(pending.values())

    def stats(self, code: str) -> dict:
        self.flush_clicks()
        conn = self._conn(self.ring.node_for(code))
        link = conn.execute("SELECT url, created_at, expires_at FROM links WHERE code = ?", (code,)).fetchone()
        if link is None:
            raise NotFound(code)
        by_day = dict(conn.execute("SELECT day, SUM(count) FROM clicks WHERE code = ? GROUP BY day ORDER BY day", (code,)).fetchall())
        by_ref = dict(conn.execute(
            "SELECT referrer, SUM(count) FROM clicks WHERE code = ? GROUP BY referrer ORDER BY 2 DESC LIMIT 10", (code,)).fetchall())
        return {"code": code, "url": link[0], "created_at": link[1], "expires_at": link[2],
                "clicks": sum(by_day.values()), "by_day": by_day, "top_referrers": by_ref}

    def shard_counts(self) -> dict[str, int]:
        return {name: self._conn(name).execute("SELECT COUNT(*) FROM links").fetchone()[0] for name in sorted(self._paths)}
