# ads_routes.py

from __future__ import annotations

import os
import datetime
import requests
from fastapi import APIRouter, HTTPException, Request

from app.ads_cache import cache_from_env, build_cache_key
from app import ads_client


# Router (merged into main FastAPI app)
router = APIRouter(prefix="/v1/ads", tags=["ads"])


# Environment
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "change_me")
SP_API_BASE_URL = os.getenv(
    "SP_API_BASE_URL",
    "http://127.0.0.1:8000/v1/pricing"
)

cache = cache_from_env()


# ---------------------------------------------------------------------------
# Cache metadata helper (returns cache status without embedding cached payload)
# ---------------------------------------------------------------------------
def cache_meta_public(meta):
    return {
        "hit": meta.hit,
        "stale": meta.stale,
        "key": meta.key,
        "created_at": meta.created_at,
        "expires_at": meta.expires_at,
    }


# ---------------------------------------------------------------------------
# Auth helper (same pattern as your existing gateway)
# ---------------------------------------------------------------------------
def verify_key(request: Request):
    key = request.headers.get("x-api-key")
    if key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


# ---------------------------------------------------------------------------
# Call existing SP-API pricing endpoint (internal call)
# ---------------------------------------------------------------------------
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
        SP_API_BASE_URL,
        params=params,
        headers={"x-api-key": GATEWAY_API_KEY},
        timeout=30,
    )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"sp_api upstream error: {resp.status_code} {resp.text}",
        )

    return resp.json()


# ---------------------------------------------------------------------------
# Compute combined metrics (safe / conservative)
# ---------------------------------------------------------------------------
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

        # Preferred normalized schema
        if spapi.get("selected_price") is not None:
            price = spapi.get("selected_price")
            currency = spapi.get("selected_currency") or spapi.get("currency")

        elif spapi.get("sales_price") is not None:
            price = spapi.get("sales_price")
            currency = spapi.get("currency")

        else:
            # Dry-run / simplified raw payload schema
            try:
                pricing = spapi["raw"]["payload"]["pricing"]
                price = pricing.get("landedPrice") or pricing.get("listingPrice")
                currency = pricing.get("currency")
            except Exception:
                pass

            # Real SP-API payload schema
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
                                landed_price.get("Amount")
                                or listing_price.get("Amount")
                                or regular_price.get("Amount")
                            )

                            currency = (
                                landed_price.get("CurrencyCode")
                                or listing_price.get("CurrencyCode")
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
            out["acos"] = float(spend) / float(sales)
        except Exception:
            out["acos"] = None
    else:
        out["acos"] = ads.get("acos") if isinstance(ads, dict) else None

    return out


# ---------------------------------------------------------------------------
# Campaign list (stub)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Report endpoint (stub)
# ---------------------------------------------------------------------------
@router.get("/report")
def get_report(
    request: Request,
    region: str,
    profileId: str,
    type: str,
    date: str,
    ttlSeconds: int = 86400,
):

    verify_key(request)

    params = {
        "type": type,
        "date": date,
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
            date,
        ),
    )

    return {
        "region": region,
        "profile_id": profileId,
        "endpoint": "report",
        "cache": cache_meta_public(meta),
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Summary endpoint (pricing + ads)
# ---------------------------------------------------------------------------
@router.get("/summary")
def ads_summary(
    request: Request,

    adsRegion: str = "NA",
    adsProfileId: str = "change_me",

    region: str = "na",
    marketplaceId: str = "ATVPDKIKX0DER",

    sku: str | None = None,
    asin: str | None = None,

    date: str | None = None,

    ttlSeconds: int = 3600,
    forceRefresh: bool = False,
):

    verify_key(request)

    if not sku and not asin:
        raise HTTPException(
            status_code=400,
            detail="Provide sku or asin",
        )

    if not date:
        date = datetime.date.today().isoformat()

    params_for_key = {

        "adsRegion": adsRegion,
        "adsProfileId": adsProfileId,

        "region": region,
        "marketplaceId": marketplaceId,

        "sku": sku or "",
        "asin": asin or "",

        "date": date,
        "version": 1,
    }

    cache_key = build_cache_key(
        ads_region=adsRegion,
        profile_id=adsProfileId,
        endpoint="ads_summary",
        params=params_for_key,
    )

    def fetch_summary():

        spapi = fetch_spapi_pricing(
            region=region,
            marketplace_id=marketplaceId,
            sku=sku,
            asin=asin,
        )

        sku_for_ads = sku or ""

        ads_perf = ads_client.fetch_campaign_performance(
            region=adsRegion,
            profile_id=adsProfileId,
            date=date,
            sku=sku_for_ads,
        )

        return {

            "identity": {

                "sku": sku or "",
                "asin": asin or "",

                "date": date,

                "marketplaceId": marketplaceId,
                "region": region,

                "adsRegion": adsRegion,
                "adsProfileId": adsProfileId,
            },

            "sp_api": spapi,
            "ads": ads_perf,

            "computed": compute_summary_metrics(
                spapi,
                ads_perf,
            ),
        }

    if forceRefresh:

        payload = fetch_summary()

        cache.set(
            cache_key,
            payload,
            ttl_seconds=ttlSeconds,
        )

        meta = cache.get(
            cache_key,
            allow_stale=True,
        )

        return {
            "endpoint": "ads_summary",
            "cache": cache_meta_public(meta),
            "payload": payload,
        }

    meta, payload = cache.get_or_fetch(
        cache_key=cache_key,
        ttl_seconds=ttlSeconds,
        fetch_fn=fetch_summary,
        allow_stale=False,
        refresh_if_stale=True,
    )

    return {
        "endpoint": "ads_summary",
        "cache": cache_meta_public(meta),
        "payload": payload,
    }
