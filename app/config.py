from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Gateway auth
    gateway_api_key: str = Field(alias="GATEWAY_API_KEY")

    # LWA
    lwa_client_id: str | None = Field(default=None, alias="LWA_CLIENT_ID")
    lwa_client_secret: str | None = Field(default=None, alias="LWA_CLIENT_SECRET")

    lwa_refresh_token_na: str | None = Field(default=None, alias="LWA_REFRESH_TOKEN_NA")
    lwa_refresh_token_eu: str | None = Field(default=None, alias="LWA_REFRESH_TOKEN_EU")
    lwa_refresh_token_fe: str | None = Field(default=None, alias="LWA_REFRESH_TOKEN_FE")

    # SP-API hosts
    spapi_host_na: str = Field(default="sellingpartnerapi-na.amazon.com", alias="SPAPI_HOST_NA")
    spapi_host_eu: str = Field(default="sellingpartnerapi-eu.amazon.com", alias="SPAPI_HOST_EU")
    spapi_host_fe: str = Field(default="sellingpartnerapi-fe.amazon.com", alias="SPAPI_HOST_FE")

    # Ads API configuration
    application_id: str | None = Field(default=None, alias="APPLICATION_ID")
    ads_client_secret: str | None = Field(default=None, alias="ADS_CLIENT_SECRET")

    dry_run: bool = Field(default=False, alias="DRY_RUN")

    # app/config.py (inside your Settings class)
    from pydantic import Field

    ads_region: str = Field(default="NA", alias="ADS_REGION")
    ads_profile_id: str = Field(default="change_me", alias="ADS_PROFILE_ID")
    ads_client_id: str = Field(default="change_me", alias="ADS_CLIENT_ID")
    ads_refresh_token_path: str = Field(default="./secrets/refresh_token.txt", alias="ADS_REFRESH_TOKEN_PATH")

    ads_cache_db_path: str = Field(default="./local_cache/ads_cache.sqlite3", alias="ADS_CACHE_DB_PATH")
    ads_cache_ttl_seconds: int = Field(default=86400, alias="ADS_CACHE_TTL_SECONDS")
    ads_cache_debug: bool = Field(default=False, alias="ADS_CACHE_DEBUG")

    sp_api_base_url: str = Field(default="http://127.0.0.1:8000/v1/pricing", alias="SP_API_BASE_URL")


settings = Settings()


def host_for_region(region: str) -> str:
    r = region.lower().strip()
    if r == "na":
        return settings.spapi_host_na
    if r == "eu":
        return settings.spapi_host_eu
    if r == "fe":
        return settings.spapi_host_fe
    raise ValueError(f"Unsupported region '{region}'. Use one of: na, eu, fe")


def refresh_token_for_region(region: str) -> str:
    r = region.lower().strip()
    token = None
    if r == "na":
        token = settings.lwa_refresh_token_na
    elif r == "eu":
        token = settings.lwa_refresh_token_eu
    elif r == "fe":
        token = settings.lwa_refresh_token_fe

    if not token:
        raise ValueError(f"Missing refresh token for region '{region}'. Set LWA_REFRESH_TOKEN_{r.upper()}.")
    return token
