# ads_client.py
from __future__ import annotations
from typing import Any, Dict


def fetch_campaigns(region: str, profile_id: str) -> Dict[str, Any]:
    """
    Stub for listing campaigns.
    Replace later with real Amazon Ads API call.
    """
    return {
        "stub": True,
        "region": region,
        "profile_id": profile_id,
        "campaigns": []
    }


def fetch_report(region: str, profile_id: str, report_type: str, date: str) -> Dict[str, Any]:
    """
    Stub for generic reports.
    """
    return {
        "stub": True,
        "region": region,
        "profile_id": profile_id,
        "type": report_type,
        "date": date,
        "rows": []
    }


def fetch_campaign_performance(region: str, profile_id: str, date: str, sku: str) -> Dict[str, Any]:
    """
    Stub used by /v1/ads/summary

    Later this will:
    - Request report from Amazon Ads API
    - Download report file
    - Aggregate performance for the SKU / ASIN
    """
    return {
        "stub": True,
        "region": region,
        "profile_id": profile_id,
        "date": date,
        "sku": sku,

        # placeholders matching expected real output
        "spend": 0.0,
        "sales": 0.0,
        "clicks": 0,
        "impressions": 0,
        "orders": 0,
        "acos": None,
    }
