import httpx
from app.config import host_for_region


class AmazonSpApiError(RuntimeError):
    def __init__(self, status_code: int, body: dict | str):
        super().__init__(f"SP-API error {status_code}")
        self.status_code = status_code
        self.body = body


async def spapi_get_pricing(
    *,
    region: str,
    lwa_access_token: str,
    marketplace_id: str,
    asin: str | None,
    sku: str | None,
) -> dict:
    host = host_for_region(region)
    url = f"https://{host}/products/pricing/v0/price"

    # Amazon expects *either* Asins or Skus depending on ItemType
    if asin:
        params = {
            "MarketplaceId": marketplace_id,
            "ItemType": "Asin",
            "Asins": asin,
        }
    else:
        params = {
            "MarketplaceId": marketplace_id,
            "ItemType": "Sku",
            "Skus": sku,
        }

    headers = {
        "x-amz-access-token": lwa_access_token,
        "accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, params=params, headers=headers)

    if r.status_code >= 400:
        body = None
        try:
            body = r.json()
        except Exception:
            body = r.text
        raise AmazonSpApiError(r.status_code, body)

    return r.json()
