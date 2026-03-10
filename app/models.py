from pydantic import BaseModel, Field


class PricingNormalized(BaseModel):
    input_type: str = Field(..., description="asin|sku")
    input_value: str
    marketplace_id: str
    region: str

    # We'll keep these flexible because Amazon response shapes vary
    summary: dict = Field(default_factory=dict)
    raw: dict
