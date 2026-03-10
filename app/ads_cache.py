"""
ads_cache.py - SQLite-backed cache for Amazon Ads API responses (or any payloads)

Design goals:
- Works now with placeholder creds (fetch layer can be a stub)
- Safe on a single droplet (SQLite WAL + basic locking)
- Easy TTL + pruning
- Payload stored as JSON text (dict/list -> json)
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Optional, Dict, Tuple


def _now() -> int:
    return int(time.time())


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def build_cache_key(
    *,
    ads_region: str,
    profile_id: str,
    endpoint: str,
    params: Dict[str, Any],
) -> str:
    """
    Creates a stable cache key. Normalize params to a canonical JSON string,
    then hash to keep keys short and safe for indexing.
    """
    canonical = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    raw = f"{ads_region}|{profile_id}|{endpoint}|{canonical}"
    return _sha256(raw)


@dataclass(frozen=True)
class CacheResult:
    hit: bool
    stale: bool
    key: str
    payload: Optional[Any]
    created_at: Optional[int]
    expires_at: Optional[int]


class AdsCache:
    def __init__(
        self,
        db_path: str,
        default_ttl_seconds: int = 86400,
        debug: bool = False,
    ) -> None:
        self.db_path = db_path
        self.default_ttl_seconds = int(default_ttl_seconds)
        self.debug = bool(debug)

        # A simple lock to serialize writes; reads can be concurrent.
        self._write_lock = threading.Lock()

        # Ensure folder exists
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        # WAL mode improves concurrency on a single-host sqlite db
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ads_cache (
                    cache_key   TEXT PRIMARY KEY,
                    created_at  INTEGER NOT NULL,
                    expires_at  INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ads_cache_expires_at ON ads_cache(expires_at);"
            )

    def get(self, cache_key: str, allow_stale: bool = False) -> CacheResult:
        """
        Return cached payload if present and not expired.
        If allow_stale=True, returns payload even if expired (marked stale=True).
        """
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cache_key, created_at, expires_at, payload_json FROM ads_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()

        if not row:
            return CacheResult(False, False, cache_key, None, None, None)

        created_at = int(row["created_at"])
        expires_at = int(row["expires_at"])
        stale = expires_at <= now

        if stale and not allow_stale:
            return CacheResult(False, True, cache_key, None, created_at, expires_at)

        payload = json.loads(row["payload_json"])
        return CacheResult(True, stale, cache_key, payload, created_at, expires_at)

    def set(self, cache_key: str, payload: Any, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        now = _now()
        expires_at = now + max(1, ttl)

        payload_json = json.dumps(payload, ensure_ascii=False)

        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO ads_cache(cache_key, created_at, expires_at, payload_json)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        created_at=excluded.created_at,
                        expires_at=excluded.expires_at,
                        payload_json=excluded.payload_json;
                    """,
                    (cache_key, now, expires_at, payload_json),
                )

        if self.debug:
            print(f"[ads_cache] set key={cache_key[:10]}... ttl={ttl}s expires_at={expires_at}")

    def delete(self, cache_key: str) -> int:
        with self._write_lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM ads_cache WHERE cache_key = ?", (cache_key,))
                return int(cur.rowcount or 0)

    def prune_expired(self) -> int:
        now = _now()
        with self._write_lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM ads_cache WHERE expires_at <= ?", (now,))
                return int(cur.rowcount or 0)

    def stats(self) -> Dict[str, Any]:
        now = _now()
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) AS n FROM ads_cache").fetchone()["n"]
            expired = conn.execute(
                "SELECT COUNT(*) AS n FROM ads_cache WHERE expires_at <= ?",
                (now,),
            ).fetchone()["n"]
        return {"total": int(total), "expired": int(expired), "now": now}

    def get_or_fetch(
        self,
        *,
        cache_key: str,
        fetch_fn: Callable[[], Any],
        ttl_seconds: Optional[int] = None,
        allow_stale: bool = False,
        refresh_if_stale: bool = True,
    ) -> Tuple[CacheResult, Any]:
        """
        Read-through cache:
        - If fresh hit -> returns cached
        - If miss -> fetch -> store -> return
        - If stale:
            - if allow_stale and not refresh_if_stale -> return stale cached
            - else fetch -> store -> return
        """
        cached = self.get(cache_key, allow_stale=allow_stale)

        if cached.hit and not cached.stale:
            return cached, cached.payload

        if cached.hit and cached.stale and allow_stale and not refresh_if_stale:
            return cached, cached.payload

        # Miss or stale (and we want refresh)
        data = fetch_fn()
        self.set(cache_key, data, ttl_seconds=ttl_seconds)
        fresh = self.get(cache_key, allow_stale=True)
        return fresh, data


from app.config import settings

def cache_from_env() -> AdsCache:
    return AdsCache(
        db_path=settings.ads_cache_db_path or "./local_cache/ads_cache.db",
        default_ttl_seconds=settings.ads_cache_ttl_seconds,
        debug=settings.ads_cache_debug,
    )
