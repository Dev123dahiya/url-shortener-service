"""Consistent hashing with virtual nodes."""
from __future__ import annotations

import bisect
import hashlib


def _hash(key: str) -> int:
    return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "big")


class HashRing:
    """Maps keys to nodes so that adding or removing a node moves only ~1/N of keys.

    Each node is placed on the ring ``vnodes`` times to even out the load.
    """

    def __init__(self, nodes: list[str] | None = None, vnodes: int = 128) -> None:
        self.vnodes = vnodes
        self._points: list[int] = []
        self._owner: dict[int, str] = {}
        self.nodes: set[str] = set()
        for node in nodes or []:
            self.add(node)

    def add(self, node: str) -> None:
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.vnodes):
            point = _hash(f"{node}#{i}")
            self._owner[point] = node
            bisect.insort(self._points, point)

    def remove(self, node: str) -> None:
        if node not in self.nodes:
            return
        self.nodes.discard(node)
        for i in range(self.vnodes):
            point = _hash(f"{node}#{i}")
            del self._owner[point]
            self._points.pop(bisect.bisect_left(self._points, point))

    def node_for(self, key: str) -> str:
        if not self._points:
            raise LookupError("ring is empty")
        i = bisect.bisect(self._points, _hash(key)) % len(self._points)
        return self._owner[self._points[i]]
