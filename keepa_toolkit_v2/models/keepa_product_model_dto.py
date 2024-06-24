from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class KeepaProductModelDto(BaseModel):
    title: str
    asin: str
    root_category: str | None
    brand: str | None
    url: str
    count_on_amazon: int = Field(default=0)
    buy_box_90_days_avg: Decimal = Field(default=0)
    new_offer_count_current: int = Field(default=0)
    fba_fee: Decimal = Field(default=0)
    referral_fee: Decimal
    package_height: float = Field(default=-1)
    package_width: float = Field(default=-1)
    package_length: float = Field(default=-1)
    package_weight: float = Field(default=-1)
    sales_rank_current: int = Field(default=0)
    reviews_rating: Decimal | None
    reviews_count: int | None
    reviews_count_30_days_avg: int | None
    reviews_count_180_days_avg: int | None
    review_velocity: int
    availability_of_amazon_offer: int = Field(default=0)
    variations_count: int | None
    updated_at: str | None | date = Field(default=str(datetime.utcnow().date()))

    # TODO update it with Stepity site template
    @classmethod
    def from_tuple(cls, product_tuple):
        return cls(
            title=product_tuple[1],
            asin=product_tuple[2],
            root_category=product_tuple[3],
            brand=product_tuple[4],
            url=product_tuple[5],
            count_on_amazon=product_tuple[6],
            buy_box_90_days_avg=product_tuple[7],
            new_offer_count_current=product_tuple[8],
            fba_fee=product_tuple[9],
            referral_fee=product_tuple[10],
            package_height=product_tuple[11],
            package_width=product_tuple[12],
            package_length=product_tuple[13],
            package_weight=product_tuple[14],
            sales_rank_current=product_tuple[15],
            reviews_rating=product_tuple[16],
            reviews_count=product_tuple[17],
            reviews_count_30_days_avg=product_tuple[18],
            reviews_count_180_days_avg=product_tuple[19],
            review_velocity=product_tuple[20],
            availability_of_amazon_offer=product_tuple[21],
            variations_count=product_tuple[22],
            updated_at=product_tuple[23]
        )
