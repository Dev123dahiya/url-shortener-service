"""Collision-free IDs without a central counter, and base62 short codes."""
from __future__ import annotations

import threading
import time
from typing import Callable

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_INDEX = {c: i for i, c in enumerate(ALPHABET)}

EPOCH_MS = 1_735_689_600_000  # 2025-01-01T00:00:00Z
NODE_BITS, SEQ_BITS = 10, 12
MAX_NODE, MAX_SEQ = (1 << NODE_BITS) - 1, (1 << SEQ_BITS) - 1


def encode_base62(n: int) -> str:
    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return ALPHABET[0]
    out = []
    while n:
        n, r = divmod(n, 62)
        out.append(ALPHABET[r])
    return "".join(reversed(out))


def decode_base62(s: str) -> int:
    n = 0
    for c in s:
        if c not in _INDEX:
            raise ValueError(f"invalid base62 character {c!r}")
        n = n * 62 + _INDEX[c]
    return n


class SnowflakeGenerator:
    """64-bit IDs: 41 bits of milliseconds | 10 bits of node id | 12 bits of sequence.

    Each node mints up to 4,096 IDs per millisecond with no coordination. IDs from
    one node are strictly increasing. If the wall clock steps backwards we keep
    using the last timestamp seen rather than risk issuing a duplicate.
    """

    def __init__(self, node_id: int, clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000) -> None:
        if not 0 <= node_id <= MAX_NODE:
            raise ValueError(f"node_id must be in [0, {MAX_NODE}]")
        self.node_id = node_id
        self._clock_ms = clock_ms
        self._lock = threading.Lock()
        self._last_ms = -1
        self._seq = 0

    def next_id(self) -> int:
        with self._lock:
            now = max(self._clock_ms(), self._last_ms)  # never go backwards
            if now == self._last_ms:
                self._seq += 1
                if self._seq > MAX_SEQ:  # this millisecond is full: borrow the next one
                    now += 1
                    self._seq = 0
            else:
                self._seq = 0
            self._last_ms = now
            return ((now - EPOCH_MS) << (NODE_BITS + SEQ_BITS)) | (self.node_id << SEQ_BITS) | self._seq

    @staticmethod
    def parts(snowflake: int) -> dict[str, int]:
        return {
            "timestamp_ms": (snowflake >> (NODE_BITS + SEQ_BITS)) + EPOCH_MS,
            "node_id": (snowflake >> SEQ_BITS) & MAX_NODE,
            "sequence": snowflake & MAX_SEQ,
        }
