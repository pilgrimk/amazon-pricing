import httpx
from app.config import settings


LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"


async def fetch_lwa_access_token(refresh_token: str) -> tuple[str, int]:
    """
    Returns (access_token, expires_in_seconds)
    """
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": settings.lwa_client_id,
        "client_secret": settings.lwa_client_secret,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(LWA_TOKEN_URL, data=data)
        r.raise_for_status()
        j = r.json()

    token = j.get("access_token")
    expires_in = int(j.get("expires_in", 3600))
    if not token:
        raise RuntimeError(f"LWA response missing access_token. Response: {j}")
    return token, expires_in
