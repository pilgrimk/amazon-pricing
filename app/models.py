from pydantic import BaseModel, Field
from typing import Literal


class PricingNormalized(BaseModel):
    input_type: str = Field(..., description="asin|sku")
    input_value: str
    marketplace_id: str
    region: str

    summary: dict = Field(default_factory=dict)
    raw: dict


class AdsBatchItem(BaseModel):
    type: Literal["asin", "sku"]
    values: list[str] = Field(default_factory=list)


class AdsRefreshBatchRequest(BaseModel):
    items: list[AdsBatchItem] = Field(default_factory=list)
    asyncMode: bool = False