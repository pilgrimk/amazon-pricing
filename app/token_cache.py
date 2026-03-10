import time
import asyncio
from dataclasses import dataclass


@dataclass
class CacheEntry:
    value: str
    expires_at: float  # epoch seconds


class InMemoryTokenCache:
    """
    Simple in-memory cache with a lock to avoid token stampedes under load.
    Good enough for single instance. If you scale horizontally, move to Redis.
    """
    def __init__(self):
        self._store: dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

    def get(self, key: str) -> str | None:
        ent = self._store.get(key)
        if not ent:
            return None
        if time.time() >= ent.expires_at:
            self._store.pop(key, None)
            return None
        return ent.value

    async def get_or_set(self, key: str, fetch_fn, ttl_seconds: int) -> str:
        # First quick check without lock
        existing = self.get(key)
        if existing:
            return existing

        async with self._lock:
            # Re-check after acquiring lock
            existing2 = self.get(key)
            if existing2:
                return existing2

            value = await fetch_fn()
            # Safety buffer: if ttl_seconds is 3600, cache for 3300 etc happens at caller
            self._store[key] = CacheEntry(value=value, expires_at=time.time() + ttl_seconds)
            return value
