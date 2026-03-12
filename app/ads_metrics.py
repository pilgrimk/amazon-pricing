from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.config import settings


@dataclass
class CoverageResult:
    expected_days: int
    found_days: int
    complete: bool
    min_date: str | None
    max_date: str | None
    last_updated_at: str | None


def _to_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except Exception:
        return 0


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


class AdsMetricsStore:
    def __init__(self, db_path: str, debug: bool = False):
        self.db_path = db_path
        self.debug = debug
        self._ensure_parent_dir()
        self._init_db()

    def _ensure_parent_dir(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ads_daily_metrics (
                    date TEXT NOT NULL,
                    ads_region TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    sku TEXT NOT NULL DEFAULT '',
                    asin TEXT NOT NULL DEFAULT '',
                    impressions INTEGER NOT NULL DEFAULT 0,
                    clicks INTEGER NOT NULL DEFAULT 0,
                    spend REAL NOT NULL DEFAULT 0,
                    sales REAL NOT NULL DEFAULT 0,
                    orders_count INTEGER NOT NULL DEFAULT 0,
                    units INTEGER NOT NULL DEFAULT 0,
                    last_updated_at TEXT NOT NULL,
                    PRIMARY KEY (date, ads_region, profile_id, sku, asin)
                )
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_lookup
                ON ads_daily_metrics (ads_region, profile_id, date)
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_sku
                ON ads_daily_metrics (ads_region, profile_id, sku, date)
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_asin
                ON ads_daily_metrics (ads_region, profile_id, asin, date)
                """
            )

            conn.commit()

    def upsert_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        normalized_rows: list[tuple[Any, ...]] = []
        for row in rows:
            normalized_rows.append(
                (
                    str(row.get("date") or "").strip(),
                    str(row.get("ads_region") or "").strip(),
                    str(row.get("profile_id") or "").strip(),
                    str(row.get("sku") or "").strip(),
                    str(row.get("asin") or "").strip(),
                    _to_int(row.get("impressions")),
                    _to_int(row.get("clicks")),
                    round(_to_float(row.get("spend")), 2),
                    round(_to_float(row.get("sales")), 2),
                    _to_int(row.get("orders")),
                    _to_int(row.get("units")),
                    now,
                )
            )

        filtered_rows = [
            row for row in normalized_rows
            if row[0] and row[1] and row[2] and (row[3] or row[4])
        ]

        if not filtered_rows:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO ads_daily_metrics (
                    date,
                    ads_region,
                    profile_id,
                    sku,
                    asin,
                    impressions,
                    clicks,
                    spend,
                    sales,
                    orders_count,
                    units,
                    last_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, ads_region, profile_id, sku, asin)
                DO UPDATE SET
                    impressions = excluded.impressions,
                    clicks = excluded.clicks,
                    spend = excluded.spend,
                    sales = excluded.sales,
                    orders_count = excluded.orders_count,
                    units = excluded.units,
                    last_updated_at = excluded.last_updated_at
                """,
                filtered_rows,
            )
            conn.commit()

        return len(filtered_rows)

    def get_summary(
        self,
        *,
        ads_region: str,
        profile_id: str,
        start_date: str,
        end_date: str,
        sku: str | None = None,
        asin: str | None = None,
    ) -> dict[str, Any]:
        where_clauses = [
            "ads_region = ?",
            "profile_id = ?",
            "date >= ?",
            "date <= ?",
        ]
        params: list[Any] = [ads_region, profile_id, start_date, end_date]

        sku = (sku or "").strip()
        asin = (asin or "").strip()

        if sku and asin:
            where_clauses.append("(sku = ? OR asin = ?)")
            params.extend([sku, asin])
        elif sku:
            where_clauses.append("sku = ?")
            params.append(sku)
        elif asin:
            where_clauses.append("asin = ?")
            params.append(asin)

        sql = f"""
            SELECT
                COALESCE(SUM(impressions), 0) AS impressions,
                COALESCE(SUM(clicks), 0) AS clicks,
                COALESCE(SUM(spend), 0) AS spend,
                COALESCE(SUM(sales), 0) AS sales,
                COALESCE(SUM(orders_count), 0) AS orders_count,
                COALESCE(SUM(units), 0) AS units,
                COUNT(*) AS row_count
            FROM ads_daily_metrics
            WHERE {" AND ".join(where_clauses)}
        """

        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()

        impressions = _to_int(row["impressions"]) if row else 0
        clicks = _to_int(row["clicks"]) if row else 0
        spend = round(_to_float(row["spend"]), 2) if row else 0.0
        sales = round(_to_float(row["sales"]), 2) if row else 0.0
        orders = _to_int(row["orders_count"]) if row else 0
        units = _to_int(row["units"]) if row else 0
        row_count = _to_int(row["row_count"]) if row else 0

        acos = None
        if sales > 0:
            acos = round(spend / sales, 4)

        return {
            "impressions": impressions,
            "clicks": clicks,
            "spend": spend,
            "sales": sales,
            "orders": orders,
            "units": units,
            "acos": acos,
            "row_count": row_count,
        }

    def get_coverage(
        self,
        *,
        ads_region: str,
        profile_id: str,
        start_date: str,
        end_date: str,
        sku: str | None = None,
        asin: str | None = None,
    ) -> CoverageResult:
        start = _parse_iso_date(start_date)
        end = _parse_iso_date(end_date)
        expected_days = (end - start).days + 1

        where_clauses = [
            "ads_region = ?",
            "profile_id = ?",
            "date >= ?",
            "date <= ?",
        ]
        params: list[Any] = [ads_region, profile_id, start_date, end_date]

        sku = (sku or "").strip()
        asin = (asin or "").strip()

        if sku and asin:
            where_clauses.append("(sku = ? OR asin = ?)")
            params.extend([sku, asin])
        elif sku:
            where_clauses.append("sku = ?")
            params.append(sku)
        elif asin:
            where_clauses.append("asin = ?")
            params.append(asin)

        sql = f"""
            SELECT
                COUNT(DISTINCT date) AS found_days,
                MIN(date) AS min_date,
                MAX(date) AS max_date,
                MAX(last_updated_at) AS last_updated_at
            FROM ads_daily_metrics
            WHERE {" AND ".join(where_clauses)}
        """

        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()

        found_days = _to_int(row["found_days"]) if row else 0
        min_date = row["min_date"] if row else None
        max_date = row["max_date"] if row else None
        last_updated_at = row["last_updated_at"] if row else None

        return CoverageResult(
            expected_days=expected_days,
            found_days=found_days,
            complete=(found_days == expected_days and expected_days > 0),
            min_date=min_date,
            max_date=max_date,
            last_updated_at=last_updated_at,
        )
    
    def get_latest_stored_date(
        self,
        *,
        ads_region: str,
        profile_id: str,
        sku: str | None = None,
        asin: str | None = None,
    ) -> str | None:
        where_clauses = [
            "ads_region = ?",
            "profile_id = ?",
        ]
        params: list[Any] = [ads_region, profile_id]

        sku = (sku or "").strip()
        asin = (asin or "").strip()

        if sku and asin:
            where_clauses.append("(sku = ? OR asin = ?)")
            params.extend([sku, asin])
        elif sku:
            where_clauses.append("sku = ?")
            params.append(sku)
        elif asin:
            where_clauses.append("asin = ?")
            params.append(asin)
        else:
            return None

        sql = f"""
            SELECT MAX(date) AS latest_date
            FROM ads_daily_metrics
            WHERE {" AND ".join(where_clauses)}
        """

        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()

        if not row:
            return None

        return row["latest_date"]


def metrics_store_from_env() -> AdsMetricsStore:
    db_path = getattr(
        settings,
        "ads_daily_metrics_db_path",
        "./local_cache/ads_daily_metrics.db",
    )
    debug = bool(getattr(settings, "ads_daily_metrics_debug", False))
    return AdsMetricsStore(db_path=db_path, debug=debug)