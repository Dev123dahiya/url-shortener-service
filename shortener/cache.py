"""Thread-safe LRU cache with per-entry TTL and hit/miss counters."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Hashable

_MISSING = object()


class LRUCache:
    def __init__(self, capacity: int, ttl: float | None = None, clock: Callable[[], float] = time.monotonic) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.ttl = ttl
        self._clock = clock
        self._lock = threading.Lock()
        self._data: OrderedDict[Hashable, tuple[Any, float | None]] = OrderedDict()
        self.hits = self.misses = self.evictions = 0

    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._lock:
            item = self._data.get(key, _MISSING)
            if item is not _MISSING:
                value, expires = item
                if expires is None or expires > self._clock():
                    self._data.move_to_end(key)
                    self.hits += 1
                    return value
                del self._data[key]  # expired
            self.misses += 1
            return default

    def put(self, key: Hashable, value: Any) -> None:
        with self._lock:
            expires = self._clock() + self.ttl if self.ttl is not None else None
            self._data[key] = (value, expires)
            self._data.move_to_end(key)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)
                self.evictions += 1

    def delete(self, key: Hashable) -> None:
        with self._lock:
            self._data.pop(key, None)

    def __len__(self) -> int:
        return len(self._data)

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0
