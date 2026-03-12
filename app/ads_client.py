from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime, timedelta, date
import gzip
import json
import threading
import time

import requests

from app.config import settings


_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
_PROFILES_URL = "https://advertising-api.amazon.com/v2/profiles"
_REPORTS_URL = "https://advertising-api.amazon.com/reporting/reports"

MAX_HISTORY_DAYS = 95
MAX_DAYS_PER_CHUNK = 31
DEFAULT_REPORT_MAX_WAIT_SECONDS = settings.ads_report_max_wait_seconds

# Simple in-process token cache
_token_lock = threading.Lock()
_token_cache: dict[str, Any] = {
    "access_token": None,
    "expires_at": 0,
}


def _read_ads_refresh_token() -> str:
    if not settings.ads_refresh_token_path:
        raise RuntimeError("ADS_REFRESH_TOKEN_PATH is not configured")

    with open(settings.ads_refresh_token_path, "r", encoding="utf-8") as f:
        token = f.read().strip()

    if not token:
        raise RuntimeError("ADS refresh token file is empty")

    return token


def get_ads_access_token(force_refresh: bool = False) -> str:
    """
    Return a cached Amazon Ads access token if still valid.
    Refresh only when missing, expired, or force_refresh=True.
    """
    now = int(time.time())
    refresh_buffer_seconds = 120

    with _token_lock:
        cached_token = _token_cache.get("access_token")
        expires_at = int(_token_cache.get("expires_at") or 0)

        if (
            not force_refresh
            and cached_token
            and now < (expires_at - refresh_buffer_seconds)
        ):
            return cached_token

        refresh_token = _read_ads_refresh_token()

        if not settings.ads_client_id:
            raise RuntimeError("ADS_CLIENT_ID is not configured")
        if not settings.ads_client_secret:
            raise RuntimeError("ADS_CLIENT_SECRET is not configured")

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": settings.ads_client_id,
            "client_secret": settings.ads_client_secret,
        }

        resp = requests.post(_TOKEN_URL, data=payload, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"Ads token request failed: status={resp.status_code}, body={resp.text}"
            )

        data = resp.json()
        access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))

        _token_cache["access_token"] = access_token
        _token_cache["expires_at"] = now + expires_in

        return access_token


def build_ads_headers(profile_id: str, force_token_refresh: bool = False) -> Dict[str, str]:
    access_token = get_ads_access_token(force_refresh=force_token_refresh)

    return {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": settings.ads_client_id or "",
        "Amazon-Advertising-API-Scope": profile_id,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _parse_iso_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def _compute_roll_window(
    *,
    latest_stored_date: str | None,
    bootstrap_days: int,
    lag_days: int,
) -> tuple[str, str] | None:
    today = datetime.utcnow().date()
    target_end = today - timedelta(days=lag_days)

    if latest_stored_date:
        latest = _parse_iso_date(latest_stored_date)
        next_start = latest + timedelta(days=1)

        if next_start > target_end:
            return None

        return next_start.isoformat(), target_end.isoformat()

    bootstrap_start = target_end - timedelta(days=max(bootstrap_days - 1, 0))
    return bootstrap_start.isoformat(), target_end.isoformat()


def _validate_history_window(
    start_date: str,
    end_date: str,
    max_days: int = MAX_HISTORY_DAYS,
) -> tuple[str, str]:
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)

    if end < start:
        raise RuntimeError("endDate must be greater than or equal to startDate")

    total_days = (end - start).days + 1
    if total_days > max_days:
        raise RuntimeError(f"Maximum supported historical window is {max_days} days")

    return start.isoformat(), end.isoformat()


def _request_report(
    profile_id: str,
    start_date: str,
    end_date: str,
    *,
    time_unit: str = "SUMMARY",
    force_token_refresh: bool = False,
) -> str:
    """
    Create a v3 Sponsored Products Advertised Product report request.
    Returns reportId.
    """
    headers = build_ads_headers(
        profile_id=profile_id,
        force_token_refresh=force_token_refresh,
    )

    columns = [
        "advertisedSku",
        "advertisedAsin",
        "impressions",
        "clicks",
        "cost",
        "purchases14d",
        "sales14d",
        "unitsSoldClicks14d",
    ]

    if time_unit.upper() == "DAILY":
        columns = ["date"] + columns

    body = {
        "name": f"spAdvertisedProduct_{time_unit.lower()}_{start_date}_{end_date}",
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["advertiser"],
            "columns": columns,
            "reportTypeId": "spAdvertisedProduct",
            "timeUnit": time_unit.upper(),
            "format": "GZIP_JSON",
        },
    }

    resp = requests.post(_REPORTS_URL, headers=headers, json=body, timeout=30)
    if not resp.ok:
        raise RuntimeError(
            f"Ads report create failed: status={resp.status_code}, body={resp.text}"
        )

    data = resp.json()
    report_id = data.get("reportId")
    if not report_id:
        raise RuntimeError(f"Ads report create succeeded but no reportId returned: {data}")

    return report_id


def _poll_report(
    profile_id: str,
    report_id: str,
    *,
    max_wait_seconds: int = DEFAULT_REPORT_MAX_WAIT_SECONDS,
    poll_interval_seconds: int = 5,
) -> Dict[str, Any]:
    """
    Poll report status until COMPLETED/FAILED/CANCELLED or timeout.
    """
    headers = build_ads_headers(profile_id=profile_id)
    url = f"{_REPORTS_URL}/{report_id}"

    deadline = time.time() + max_wait_seconds

    while time.time() < deadline:
        resp = requests.get(url, headers=headers, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"Ads report status failed: status={resp.status_code}, body={resp.text}"
            )

        data = resp.json()
        status = (data.get("status") or "").upper()

        if status == "COMPLETED":
            return data
        if status in {"FAILED", "FAILURE", "CANCELLED"}:
            raise RuntimeError(f"Ads report ended with status={status}: {data}")

        time.sleep(poll_interval_seconds)

    raise RuntimeError(f"Timed out waiting for Ads report {report_id}")


def _download_report(download_url: str) -> List[Dict[str, Any]]:
    """
    Download GZIP_JSON report and return parsed row list.
    """
    resp = requests.get(download_url, timeout=60)
    if not resp.ok:
        raise RuntimeError(
            f"Ads report download failed: status={resp.status_code}, body={resp.text[:500]}"
        )

    raw_bytes = resp.content

    try:
        decompressed = gzip.decompress(raw_bytes)
    except OSError:
        decompressed = raw_bytes

    text = decompressed.decode("utf-8").strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _to_float(val: Any) -> float:
    try:
        if val in (None, ""):
            return 0.0
        return float(val)
    except Exception:
        return 0.0


def _to_int(val: Any) -> int:
    try:
        if val in (None, ""):
            return 0
        return int(float(val))
    except Exception:
        return 0


def _daterange_chunks(
    start_date: str,
    end_date: str,
    max_days_per_chunk: int = MAX_DAYS_PER_CHUNK,
) -> List[tuple[str, str]]:
    """
    Split an inclusive date range into API-safe chunks.
    """
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)

    if end < start:
        raise RuntimeError("endDate must be greater than or equal to startDate")

    chunks: List[tuple[str, str]] = []
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=max_days_per_chunk - 1), end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)

    return chunks


def _aggregate_rows_for_sku(rows: List[Dict[str, Any]], sku: str) -> Dict[str, Any]:
    """
    Sum metrics for the target advertised SKU.
    """
    sku_norm = (sku or "").strip().lower()

    matched_rows = []
    matched_asins = set()

    spend = 0.0
    sales = 0.0
    clicks = 0
    impressions = 0
    orders = 0
    units = 0

    for row in rows:
        row_sku = str(row.get("advertisedSku") or "").strip().lower()
        if row_sku != sku_norm:
            continue

        matched_rows.append(row)

        asin = str(row.get("advertisedAsin") or "").strip()
        if asin:
            matched_asins.add(asin)

        spend += _to_float(row.get("cost"))
        sales += _to_float(row.get("sales14d"))
        clicks += _to_int(row.get("clicks"))
        impressions += _to_int(row.get("impressions"))
        orders += _to_int(row.get("purchases14d"))
        units += _to_int(row.get("unitsSoldClicks14d"))

    acos = None
    if sales > 0:
        acos = round(spend / sales, 4)

    return {
        "matched_row_count": len(matched_rows),
        "matched_asin": sorted(matched_asins)[0] if matched_asins else None,
        "matched_asins": sorted(matched_asins),
        "matched_rows_preview": matched_rows[:5],
        "spend": round(spend, 2),
        "sales": round(sales, 2),
        "clicks": clicks,
        "impressions": impressions,
        "orders": orders,
        "units": units,
        "acos": acos,
    }


def _aggregate_chunk_results(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    matched_asins = set()
    matched_rows_preview: List[Dict[str, Any]] = []

    total_row_count = 0
    total_matched_row_count = 0
    total_spend = 0.0
    total_sales = 0.0
    total_clicks = 0
    total_impressions = 0
    total_orders = 0
    total_units = 0

    report_ids: List[str] = []

    for result in chunk_results:
        report_id = result.get("report_id")
        if report_id:
            report_ids.append(report_id)

        total_row_count += _to_int(result.get("row_count"))
        total_matched_row_count += _to_int(result.get("matched_row_count"))
        total_spend += _to_float(result.get("spend"))
        total_sales += _to_float(result.get("sales"))
        total_clicks += _to_int(result.get("clicks"))
        total_impressions += _to_int(result.get("impressions"))
        total_orders += _to_int(result.get("orders"))
        total_units += _to_int(result.get("units"))

        for asin in result.get("matched_asins", []):
            if asin:
                matched_asins.add(str(asin))

        preview_rows = result.get("matched_rows_preview", [])
        for row in preview_rows:
            if len(matched_rows_preview) < 5:
                matched_rows_preview.append(row)

    acos = None
    if total_sales > 0:
        acos = round(total_spend / total_sales, 4)

    sorted_asins = sorted(matched_asins)

    return {
        "report_ids": report_ids,
        "row_count": total_row_count,
        "matched_row_count": total_matched_row_count,
        "matched_asin": sorted_asins[0] if sorted_asins else None,
        "matched_asins": sorted_asins,
        "matched_rows_preview": matched_rows_preview,
        "spend": round(total_spend, 2),
        "sales": round(total_sales, 2),
        "clicks": total_clicks,
        "impressions": total_impressions,
        "orders": total_orders,
        "units": total_units,
        "acos": acos,
    }


def _normalize_daily_rows(
    rows: List[Dict[str, Any]],
    *,
    ads_region: str,
    profile_id: str,
    sku: str,
    asin: str,
) -> List[Dict[str, Any]]:
    """
    Convert DAILY report rows into one normalized row per date for the target SKU/ASIN.
    If multiple report rows exist for the same date, aggregate them.
    """
    sku_norm = (sku or "").strip().lower()
    asin_norm = (asin or "").strip().lower()

    by_date: dict[str, Dict[str, Any]] = {}

    for row in rows:
        row_date = str(row.get("date") or "").strip()
        if not row_date:
            continue

        row_sku = str(row.get("advertisedSku") or "").strip()
        row_asin = str(row.get("advertisedAsin") or "").strip()

        row_sku_norm = row_sku.lower()
        row_asin_norm = row_asin.lower()

        sku_match = bool(sku_norm) and row_sku_norm == sku_norm
        asin_match = bool(asin_norm) and row_asin_norm == asin_norm

        if sku_norm and asin_norm:
            if not (sku_match or asin_match):
                continue
        elif sku_norm:
            if not sku_match:
                continue
        elif asin_norm:
            if not asin_match:
                continue
        else:
            continue

        bucket = by_date.setdefault(
            row_date,
            {
                "date": row_date,
                "ads_region": ads_region,
                "profile_id": profile_id,
                "sku": row_sku or sku,
                "asin": row_asin or asin,
                "impressions": 0,
                "clicks": 0,
                "spend": 0.0,
                "sales": 0.0,
                "orders": 0,
                "units": 0,
            },
        )

        if not bucket.get("sku") and row_sku:
            bucket["sku"] = row_sku
        if not bucket.get("asin") and row_asin:
            bucket["asin"] = row_asin

        bucket["impressions"] += _to_int(row.get("impressions"))
        bucket["clicks"] += _to_int(row.get("clicks"))
        bucket["spend"] += _to_float(row.get("cost"))
        bucket["sales"] += _to_float(row.get("sales14d"))
        bucket["orders"] += _to_int(row.get("purchases14d"))
        bucket["units"] += _to_int(row.get("unitsSoldClicks14d"))

    normalized_rows: List[Dict[str, Any]] = []
    for date_key in sorted(by_date.keys()):
        bucket = by_date[date_key]
        bucket["spend"] = round(bucket["spend"], 2)
        bucket["sales"] = round(bucket["sales"], 2)
        normalized_rows.append(bucket)

    return normalized_rows


def _fetch_campaign_performance_for_range(
    region: str,
    profile_id: str,
    start_date: str,
    end_date: str,
    sku: str,
    *,
    max_wait_seconds: int = DEFAULT_REPORT_MAX_WAIT_SECONDS,
) -> Dict[str, Any]:
    report_id = _request_report(
        profile_id=profile_id,
        start_date=start_date,
        end_date=end_date,
        time_unit="SUMMARY",
    )
    status_data = _poll_report(
        profile_id=profile_id,
        report_id=report_id,
        max_wait_seconds=max_wait_seconds,
    )

    download_url = status_data.get("url") or status_data.get("location")
    if not download_url:
        raise RuntimeError(f"Report completed but no download URL returned: {status_data}")

    rows = _download_report(download_url)
    agg = _aggregate_rows_for_sku(rows, sku)

    return {
        "stub": False,
        "region": region,
        "profile_id": profile_id,
        "start_date": start_date,
        "end_date": end_date,
        "sku": sku,
        "report_id": report_id,
        "report_status": status_data.get("status"),
        "row_count": len(rows),
        "matched_row_count": agg["matched_row_count"],
        "matched_asin": agg["matched_asin"],
        "matched_asins": agg["matched_asins"],
        "matched_rows_preview": agg["matched_rows_preview"],
        "spend": agg["spend"],
        "sales": agg["sales"],
        "clicks": agg["clicks"],
        "impressions": agg["impressions"],
        "orders": agg["orders"],
        "units": agg["units"],
        "acos": agg["acos"],
    }


def _fetch_daily_report_rows_for_range(
    profile_id: str,
    start_date: str,
    end_date: str,
    *,
    max_wait_seconds: int = DEFAULT_REPORT_MAX_WAIT_SECONDS,
) -> Dict[str, Any]:
    report_id = _request_report(
        profile_id=profile_id,
        start_date=start_date,
        end_date=end_date,
        time_unit="DAILY",
    )
    status_data = _poll_report(
        profile_id=profile_id,
        report_id=report_id,
        max_wait_seconds=max_wait_seconds,
    )

    download_url = status_data.get("url") or status_data.get("location")
    if not download_url:
        raise RuntimeError(f"Report completed but no download URL returned: {status_data}")

    rows = _download_report(download_url)

    return {
        "report_id": report_id,
        "report_status": status_data.get("status"),
        "row_count": len(rows),
        "rows": rows,
    }


def fetch_campaigns(region: str, profile_id: str) -> Dict[str, Any]:
    headers = build_ads_headers(profile_id=profile_id)

    resp = requests.get(_PROFILES_URL, headers=headers, timeout=30)
    if not resp.ok:
        raise RuntimeError(
            f"Ads API request failed: status={resp.status_code}, body={resp.text}"
        )

    return {
        "stub": False,
        "region": region,
        "profile_id": profile_id,
        "profiles": resp.json(),
    }


def fetch_report(
    region: str,
    profile_id: str,
    report_type: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Historical range report wrapper using the same chunking model as summary.
    """
    start_date, end_date = _validate_history_window(start_date, end_date)

    chunks = _daterange_chunks(start_date, end_date, max_days_per_chunk=MAX_DAYS_PER_CHUNK)

    report_payloads = []
    report_ids: List[str] = []
    total_rows = 0

    for chunk_start, chunk_end in chunks:
        report_id = _request_report(
            profile_id=profile_id,
            start_date=chunk_start,
            end_date=chunk_end,
            time_unit="SUMMARY",
        )
        status_data = _poll_report(profile_id=profile_id, report_id=report_id)

        download_url = status_data.get("url") or status_data.get("location")
        if not download_url:
            raise RuntimeError(f"Report completed but no download URL returned: {status_data}")

        rows = _download_report(download_url)

        report_ids.append(report_id)
        total_rows += len(rows)

        if len(report_payloads) < 5:
            report_payloads.extend(rows[: max(0, 5 - len(report_payloads))])

    return {
        "stub": False,
        "region": region,
        "profile_id": profile_id,
        "type": report_type,
        "start_date": start_date,
        "end_date": end_date,
        "chunk_count": len(chunks),
        "chunks": [{"start_date": s, "end_date": e} for s, e in chunks],
        "report_ids": report_ids,
        "row_count": total_rows,
        "rows_preview": report_payloads[:5],
    }


def fetch_campaign_performance(
    region: str,
    profile_id: str,
    sku: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Historical range only.
    Uses Sponsored Products v3 Advertised Product SUMMARY report.
    """
    start_date, end_date = _validate_history_window(start_date, end_date)

    chunks = _daterange_chunks(start_date, end_date, max_days_per_chunk=MAX_DAYS_PER_CHUNK)

    chunk_results = []
    for chunk_start, chunk_end in chunks:
        chunk_results.append(
            _fetch_campaign_performance_for_range(
                region=region,
                profile_id=profile_id,
                start_date=chunk_start,
                end_date=chunk_end,
                sku=sku,
                max_wait_seconds=DEFAULT_REPORT_MAX_WAIT_SECONDS,
            )
        )

    agg = _aggregate_chunk_results(chunk_results)

    return {
        "stub": False,
        "region": region,
        "profile_id": profile_id,
        "start_date": start_date,
        "end_date": end_date,
        "sku": sku,
        "chunk_count": len(chunks),
        "chunks": [{"start_date": s, "end_date": e} for s, e in chunks],
        "report_ids": agg["report_ids"],
        "report_status": "COMPLETED",
        "row_count": agg["row_count"],
        "matched_row_count": agg["matched_row_count"],
        "matched_asin": agg["matched_asin"],
        "matched_asins": agg["matched_asins"],
        "matched_rows_preview": agg["matched_rows_preview"],
        "spend": agg["spend"],
        "sales": agg["sales"],
        "clicks": agg["clicks"],
        "impressions": agg["impressions"],
        "orders": agg["orders"],
        "units": agg["units"],
        "acos": agg["acos"],
    }


def refresh_daily_metrics(
    region: str,
    profile_id: str,
    sku: str,
    asin: str,
    start_date: str,
    end_date: str,
    metrics_store,
) -> Dict[str, Any]:
    """
    Fetch DAILY Amazon Ads report data, normalize it to one row per day for the
    requested SKU/ASIN, and upsert it into the local ads_daily_metrics store.
    """
    start_date, end_date = _validate_history_window(start_date, end_date)

    chunks = _daterange_chunks(start_date, end_date, max_days_per_chunk=MAX_DAYS_PER_CHUNK)

    report_ids: List[str] = []
    total_source_rows = 0
    all_daily_rows: List[Dict[str, Any]] = []

    for chunk_start, chunk_end in chunks:
        result = _fetch_daily_report_rows_for_range(
            profile_id=profile_id,
            start_date=chunk_start,
            end_date=chunk_end,
            max_wait_seconds=DEFAULT_REPORT_MAX_WAIT_SECONDS,
        )

        report_ids.append(result["report_id"])
        total_source_rows += _to_int(result.get("row_count"))

        normalized_rows = _normalize_daily_rows(
            result.get("rows", []),
            ads_region=region,
            profile_id=profile_id,
            sku=sku,
            asin=asin,
        )

        all_daily_rows.extend(normalized_rows)

    # Re-aggregate across all chunks by date just in case
    final_rows = _normalize_daily_rows(
        [
            {
                "date": row["date"],
                "advertisedSku": row.get("sku", ""),
                "advertisedAsin": row.get("asin", ""),
                "impressions": row.get("impressions", 0),
                "clicks": row.get("clicks", 0),
                "cost": row.get("spend", 0.0),
                "purchases14d": row.get("orders", 0),
                "sales14d": row.get("sales", 0.0),
                "unitsSoldClicks14d": row.get("units", 0),
            }
            for row in all_daily_rows
        ],
        ads_region=region,
        profile_id=profile_id,
        sku=sku,
        asin=asin,
    )

    rows_upserted = metrics_store.upsert_rows(final_rows)

    summary = metrics_store.get_summary(
        ads_region=region,
        profile_id=profile_id,
        start_date=start_date,
        end_date=end_date,
        sku=sku or None,
        asin=asin or None,
    )

    coverage = metrics_store.get_coverage(
        ads_region=region,
        profile_id=profile_id,
        start_date=start_date,
        end_date=end_date,
        sku=sku or None,
        asin=asin or None,
    )

    return {
        "stub": False,
        "region": region,
        "profile_id": profile_id,
        "sku": sku,
        "asin": asin,
        "start_date": start_date,
        "end_date": end_date,
        "chunk_count": len(chunks),
        "chunks": [{"start_date": s, "end_date": e} for s, e in chunks],
        "report_ids": report_ids,
        "source_row_count": total_source_rows,
        "rows_upserted": rows_upserted,
        "daily_row_count": len(final_rows),
        "coverage": {
            "expected_days": coverage.expected_days,
            "found_days": coverage.found_days,
            "complete": coverage.complete,
            "min_date": coverage.min_date,
            "max_date": coverage.max_date,
            "last_updated_at": coverage.last_updated_at,
        },
        "summary": {
            "impressions": summary.get("impressions", 0),
            "clicks": summary.get("clicks", 0),
            "spend": summary.get("spend", 0.0),
            "sales": summary.get("sales", 0.0),
            "orders": summary.get("orders", 0),
            "units": summary.get("units", 0),
            "acos": summary.get("acos"),
            "row_count": summary.get("row_count", 0),
        },
    }


def refresh_daily_metrics_roll(
    region: str,
    profile_id: str,
    sku: str,
    asin: str,
    metrics_store,
    bootstrap_days: int | None = None,
    lag_days: int | None = None,
) -> Dict[str, Any]:
    """
    Rolling daily refresh.
    - If data already exists, fetch only missing days through yesterday.
    - If no data exists, bootstrap a configurable trailing window.
    """
    bootstrap_days = bootstrap_days or settings.ads_refresh_bootstrap_days
    lag_days = lag_days if lag_days is not None else settings.ads_refresh_lag_days

    latest_stored_date = metrics_store.get_latest_stored_date(
        ads_region=region,
        profile_id=profile_id,
        sku=sku or None,
        asin=asin or None,
    )

    window = _compute_roll_window(
        latest_stored_date=latest_stored_date,
        bootstrap_days=bootstrap_days,
        lag_days=lag_days,
    )

    if window is None:
        today = datetime.utcnow().date()
        target_end = today - timedelta(days=lag_days)

        return {
            "stub": False,
            "region": region,
            "profile_id": profile_id,
            "sku": sku,
            "asin": asin,
            "already_current": True,
            "latest_stored_date": latest_stored_date,
            "target_end_date": target_end.isoformat(),
            "rows_upserted": 0,
            "daily_row_count": 0,
            "coverage": None,
            "summary": None,
        }

    start_date, end_date = window

    refresh_result = refresh_daily_metrics(
        region=region,
        profile_id=profile_id,
        sku=sku,
        asin=asin,
        start_date=start_date,
        end_date=end_date,
        metrics_store=metrics_store,
    )

    refresh_result["already_current"] = False
    refresh_result["latest_stored_date_before_refresh"] = latest_stored_date
    refresh_result["roll_window"] = {
        "start_date": start_date,
        "end_date": end_date,
        "bootstrap_days": bootstrap_days,
        "lag_days": lag_days,
    }

    return refresh_result