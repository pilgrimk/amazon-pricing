from fastapi import FastAPI, Header, HTTPException, Query
from app.ads_routes import router as ads_router
from app.config import settings, refresh_token_for_region
from app.token_cache import InMemoryTokenCache
from app.lwa import fetch_lwa_access_token
from app.amazon import spapi_get_pricing, AmazonSpApiError
from app.models import PricingNormalized
import time

app = FastAPI(title="SP-API Gateway", version="1.0.0")
app.include_router(ads_router)

token_cache = InMemoryTokenCache()


def require_api_key(x_api_key: str | None):
    if not x_api_key or x_api_key != settings.gateway_api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


def validate_region(region: str):
    r = region.lower().strip()
    if r not in {"na", "eu", "fe"}:
        raise HTTPException(status_code=400, detail="Invalid region. Use one of: na, eu, fe")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/pricing", response_model=PricingNormalized)
async def pricing(
    region: str = Query(..., description="na|eu|fe"),
    marketplaceId: str = Query(..., description="MarketplaceId, e.g., ATVPDKIKX0DER"),
    asin: str | None = Query(default=None),
    sku: str | None = Query(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
):
    require_api_key(x_api_key)

    # Validate input: exactly one of asin or sku
    if bool(asin) == bool(sku):
        raise HTTPException(status_code=400, detail="Provide exactly one of: asin or sku")

    # Validate region BEFORE any processing (even DRY_RUN)
    validate_region(region)

    # ---------------------------
    # DRY RUN MODE
    # ---------------------------
    if getattr(settings, "dry_run", False):
        input_type = "asin" if asin else "sku"
        input_value = asin or sku or ""

        raw = {
            "payload": {
                "input": {
                    "type": input_type,
                    "value": input_value,
                    "marketplaceId": marketplaceId,
                    "region": region,
                },
                "pricing": {
                    "currency": "USD",
                    "listingPrice": 19.99,
                    "shipping": 0.00,
                    "landedPrice": 19.99,
                },
                "generatedAt": int(time.time()),
            }
        }

        return PricingNormalized(
            input_type=input_type,
            input_value=input_value,
            marketplace_id=marketplaceId,
            region=region.lower().strip(),
            summary={"dry_run": True},
            raw=raw,
        )

    # ---------------------------
    # REAL AMAZON CALL
    # ---------------------------

    try:
        refresh_token = refresh_token_for_region(region)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    cache_key = f"lwa:{region}:{refresh_token[-8:]}"

    ttl_holder = {"ttl": 3300}

    async def _fetch():
        access_token, expires_in = await fetch_lwa_access_token(refresh_token)
        ttl_holder["ttl"] = max(60, expires_in - 300)
        return access_token

    lwa_access_token = await token_cache.get_or_set(
        cache_key, _fetch, ttl_seconds=ttl_holder["ttl"]
    )

    try:
        raw = await spapi_get_pricing(
            region=region,
            lwa_access_token=lwa_access_token,
            marketplace_id=marketplaceId,
            asin=asin,
            sku=sku,
        )
    except AmazonSpApiError as e:
        raise HTTPException(status_code=e.status_code, detail=e.body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    input_type = "asin" if asin else "sku"
    input_value = asin or sku or ""

    summary = {
        "has_payload": "payload" in raw,
        "has_errors": "errors" in raw,
    }

    return PricingNormalized(
        input_type=input_type,
        input_value=input_value,
        marketplace_id=marketplaceId,
        region=region.lower().strip(),
        summary=summary,
        raw=raw,
    )
