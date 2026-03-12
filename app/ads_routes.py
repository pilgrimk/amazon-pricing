from __future__ import annotations

import datetime
import requests
from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.ads_cache import cache_from_env, build_cache_key
from app.ads_metrics import metrics_store_from_env
from app import ads_client


router = APIRouter(prefix="/v1/ads", tags=["ads"])

MAX_HISTORY_DAYS = 95

cache = cache_from_env()
metrics_store = metrics_store_from_env()


def cache_meta_public(meta):
    return {
        "hit": meta.hit,
        "stale": meta.stale,
        "key": meta.key,
        "created_at": meta.created_at,
        "expires_at": meta.expires_at,
    }


def verify_key(request: Request):
    key = request.headers.get("x-api-key")
    if key != settings.gateway_api_key:
        raise HTTPException(status_code=401, detail="invalid api key")


def validate_date_range(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=400,
            detail="Both startDate and endDate are required",
        )

    try:
        start = datetime.date.fromisoformat(start_date)
        end = datetime.date.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="startDate and endDate must be in YYYY-MM-DD format",
        )

    if end < start:
        raise HTTPException(
            status_code=400,
            detail="endDate must be greater than or equal to startDate",
        )

    day_count = (end - start).days + 1
    if day_count > MAX_HISTORY_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum supported historical window is {MAX_HISTORY_DAYS} days",
        )

    return start.isoformat(), end.isoformat()


def fetch_spapi_pricing(
    region: str,
    marketplace_id: str,
    sku: str | None,
    asin: str | None,
) -> dict:
    params = {
        "region": region,
        "marketplaceId": marketplace_id,
    }

    if sku:
        params["sku"] = sku

    if asin:
        params["asin"] = asin

    resp = requests.get(
        settings.sp_api_base_url,
        params=params,
        headers={"x-api-key": settings.gateway_api_key},
        timeout=30,
    )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"sp_api upstream error: {resp.status_code} {resp.text}",
        )

    return resp.json()


def compute_summary_metrics(spapi: dict, ads: dict) -> dict:
    """
    Extract unified metrics from SP-API pricing + Ads performance.
    Supports:
    - normalized schema (selected_price, sales_price)
    - raw SP-API dry_run schema (raw.payload.pricing.landedPrice)
    - real SP-API getPricing schema (raw.payload[0].Product.Offers[0].BuyingPrice.ListingPrice)
    """

    out = {}

    price = None
    currency = None

    if isinstance(spapi, dict):
        if spapi.get("selected_price") is not None:
            price = spapi.get("selected_price")
            currency = spapi.get("selected_currency") or spapi.get("currency")

        elif spapi.get("sales_price") is not None:
            price = spapi.get("sales_price")
            currency = spapi.get("currency")

        else:
            try:
                pricing = spapi["raw"]["payload"]["pricing"]
                price = pricing.get("landedPrice") or pricing.get("listingPrice")
                currency = pricing.get("currency")
            except Exception:
                pass

            if price is None:
                try:
                    payload = spapi["raw"]["payload"]
                    if isinstance(payload, list) and payload:
                        first_item = payload[0]
                        offers = (
                            first_item.get("Product", {}).get("Offers", [])
                            if isinstance(first_item, dict) else []
                        )
                        if offers:
                            first_offer = offers[0]

                            buying_price = first_offer.get("BuyingPrice", {})
                            listing_price = buying_price.get("ListingPrice", {})
                            landed_price = buying_price.get("LandedPrice", {})
                            regular_price = first_offer.get("RegularPrice", {})

                            price = (
                                listing_price.get("Amount")
                                or landed_price.get("Amount")
                                or regular_price.get("Amount")
                            )

                            currency = (
                                listing_price.get("CurrencyCode")
                                or landed_price.get("CurrencyCode")
                                or regular_price.get("CurrencyCode")
                            )
                except Exception:
                    pass

    out["price"] = price
    out["currency"] = currency

    spend = ads.get("spend") if isinstance(ads, dict) else None
    sales = ads.get("sales") if isinstance(ads, dict) else None

    out["spend"] = spend
    out["ad_sales"] = sales

    if spend is not None and sales not in (None, 0):
        try:
            out["acos"] = round(float(spend) / float(sales), 4)
        except Exception:
            out["acos"] = None
    else:
        out["acos"] = ads.get("acos") if isinstance(ads, dict) else None

    return out


@router.get("/campaigns")
def get_campaigns(
    request: Request,
    region: str,
    profileId: str,
):
    verify_key(request)

    cache_key = build_cache_key(
        ads_region=region,
        profile_id=profileId,
        endpoint="campaigns",
        params={},
    )

    meta, payload = cache.get_or_fetch(
        cache_key=cache_key,
        ttl_seconds=86400,
        fetch_fn=lambda: ads_client.fetch_campaigns(
            region,
            profileId,
        ),
    )

    return {
        "region": region,
        "profile_id": profileId,
        "endpoint": "campaigns",
        "cache": cache_meta_public(meta),
        "payload": payload,
    }


@router.get("/report")
def get_report(
    request: Request,
    region: str,
    profileId: str,
    type: str,
    startDate: str,
    endDate: str,
    ttlSeconds: int = 86400,
):
    verify_key(request)
    startDate, endDate = validate_date_range(startDate, endDate)

    params = {
        "type": type,
        "startDate": startDate,
        "endDate": endDate,
    }

    cache_key = build_cache_key(
        ads_region=region,
        profile_id=profileId,
        endpoint="report",
        params=params,
    )

    meta, payload = cache.get_or_fetch(
        cache_key=cache_key,
        ttl_seconds=ttlSeconds,
        fetch_fn=lambda: ads_client.fetch_report(
            region,
            profileId,
            type,
            startDate,
            endDate,
        ),
    )

    return {
        "region": region,
        "profile_id": profileId,
        "endpoint": "report",
        "cache": cache_meta_public(meta),
        "payload": payload,
    }


@router.post("/refresh")
def refresh_ads_daily_metrics(
    request: Request,
    adsRegion: str = "NA",
    adsProfileId: str = "change_me",
    sku: str | None = None,
    asin: str | None = None,
    startDate: str | None = None,
    endDate: str | None = None,
):
    """
    Refreshes daily Ads metrics from Amazon Ads and writes them to the local
    analytics store. This endpoint is the ingestion path for nightly jobs or
    manual backfills.
    """
    verify_key(request)

    if not sku and not asin:
        raise HTTPException(
            status_code=400,
            detail="Provide sku or asin",
        )

    startDate, endDate = validate_date_range(startDate, endDate)

    try:
        result = ads_client.refresh_daily_metrics(
            region=adsRegion,
            profile_id=adsProfileId,
            sku=sku or "",
            asin=asin or "",
            start_date=startDate,
            end_date=endDate,
            metrics_store=metrics_store,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"ads refresh failed: {exc}",
        ) from exc

    return {
        "endpoint": "ads_refresh",
        "payload": {
            "identity": {
                "sku": sku or "",
                "asin": asin or "",
                "startDate": startDate,
                "endDate": endDate,
                "adsRegion": adsRegion,
                "adsProfileId": adsProfileId,
            },
            "refresh": result,
        },
    }


@router.post("/refresh-roll")
def refresh_ads_daily_metrics_roll(
    request: Request,
    adsRegion: str = "NA",
    adsProfileId: str = "change_me",
    sku: str | None = None,
    asin: str | None = None,
    bootstrapDays: int | None = None,
):
    """
    Rolling refresh endpoint.
    Refreshes only missing days through yesterday.
    If no data exists yet, bootstraps a trailing window.
    """
    verify_key(request)

    if not sku and not asin:
        raise HTTPException(
            status_code=400,
            detail="Provide sku or asin",
        )

    try:
        result = ads_client.refresh_daily_metrics_roll(
            region=adsRegion,
            profile_id=adsProfileId,
            sku=sku or "",
            asin=asin or "",
            metrics_store=metrics_store,
            bootstrap_days=bootstrapDays,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"ads rolling refresh failed: {exc}",
        ) from exc

    return {
        "endpoint": "ads_refresh_roll",
        "payload": {
            "identity": {
                "sku": sku or "",
                "asin": asin or "",
                "adsRegion": adsRegion,
                "adsProfileId": adsProfileId,
                "bootstrapDays": bootstrapDays,
            },
            "refresh": result,
        },
    }


@router.get("/summary")
def ads_summary(
    request: Request,
    adsRegion: str = "NA",
    adsProfileId: str = "change_me",
    region: str = "na",
    marketplaceId: str = "ATVPDKIKX0DER",
    sku: str | None = None,
    asin: str | None = None,
    startDate: str | None = None,
    endDate: str | None = None,
):
    """
    Read-only analytics endpoint.
    Pricing is pulled live from the SP-API pricing endpoint.
    Ads performance is aggregated from the local daily metrics store.
    """
    verify_key(request)

    if not sku and not asin:
        raise HTTPException(
            status_code=400,
            detail="Provide sku or asin",
        )

    startDate, endDate = validate_date_range(startDate, endDate)

    try:
        spapi = fetch_spapi_pricing(
            region=region,
            marketplace_id=marketplaceId,
            sku=sku,
            asin=asin,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"sp_api pricing fetch failed: {exc}",
        ) from exc

    coverage = metrics_store.get_coverage(
        ads_region=adsRegion,
        profile_id=adsProfileId,
        start_date=startDate,
        end_date=endDate,
        sku=sku,
        asin=asin,
    )

    ads_summary_data = metrics_store.get_summary(
        ads_region=adsRegion,
        profile_id=adsProfileId,
        start_date=startDate,
        end_date=endDate,
        sku=sku,
        asin=asin,
    )

    payload = {
        "identity": {
            "sku": sku or "",
            "asin": asin or "",
            "startDate": startDate,
            "endDate": endDate,
            "marketplaceId": marketplaceId,
            "region": region,
            "adsRegion": adsRegion,
            "adsProfileId": adsProfileId,
        },
        "sp_api": spapi,
        "ads": {
            "region": adsRegion,
            "profile_id": adsProfileId,
            "start_date": startDate,
            "end_date": endDate,
            "sku": sku or "",
            "asin": asin or "",
            "impressions": ads_summary_data.get("impressions", 0),
            "clicks": ads_summary_data.get("clicks", 0),
            "spend": ads_summary_data.get("spend", 0.0),
            "sales": ads_summary_data.get("sales", 0.0),
            "orders": ads_summary_data.get("orders", 0),
            "units": ads_summary_data.get("units", 0),
            "acos": ads_summary_data.get("acos"),
            "row_count": ads_summary_data.get("row_count", 0),
            "source": "daily_metrics",
        },
        "coverage": {
            "expected_days": coverage.expected_days,
            "found_days": coverage.found_days,
            "complete": coverage.complete,
            "min_date": coverage.min_date,
            "max_date": coverage.max_date,
            "last_updated_at": coverage.last_updated_at,
        },
        "computed": compute_summary_metrics(
            spapi,
            ads_summary_data,
        ),
    }

    return {
        "endpoint": "ads_summary",
        "payload": payload,
    }