from time import sleep
from turtle import update

import frappe

import pandas as pd
from keepa import Keepa
from loguru import logger
from decimal import Decimal

from pymysql.err import IntegrityError


from keepa_toolkit_v2.common.saving_strategies import (
    LocalFileSavingStrategy,
    FireStoreSavingStrategy,
    GoogleCloudStorageSavingStrategy,
    SavingStrategyProtocol
)
from retrying import retry
from .command_base import Command
from keepa_toolkit_v2.common.utils import find_pack_number, escape_string
from keepa_toolkit_v2.models.keepa_product_model_dto import KeepaProductModelDto


def save_associative_pair(upc, asin):
    try:
        query = f"""
        INSERT INTO upc_asin (upc, asin) VALUES ('{upc}', '{asin}');
        """
        frappe.db.sql(query)
        commit = "COMMIT;"
        frappe.db.sql(commit)
        logger.debug(f"Entry {upc} -> {asin} saved.")
    except IntegrityError:
        frappe.db.rollback()
        logger.debug(f"Entry like {upc} -> {asin} already exist.")
    
def save_product_entry(product: KeepaProductModelDto):
    try:
        insert = f"""
                INSERT INTO product_compressed (
                    title, asin, root_category, brand, url, count_on_amazon, buy_box_90_days_avg, new_offer_count_current,
                    fba_fee, referral_fee, package_height, package_width, package_length, package_weight, sales_rank_current,
                    reviews_rating, reviews_count, reviews_count_30_days_avg, reviews_count_180_days_avg,
                    review_velocity, availability_of_amazon_offer, variations_count, updated_at
                )
                VALUES (
                        '{escape_string(product.title)}', '{product.asin}', '{product.root_category}',
                        '{escape_string(product.brand)}', '{product.url}',
                        {product.count_on_amazon}, {product.buy_box_90_days_avg}, {product.new_offer_count_current},
                        {product.fba_fee}, {product.referral_fee}, {product.package_height}, {product.package_width}, 
                        {product.package_length}, {product.package_weight}, {product.sales_rank_current}, {product.reviews_rating},
                        {product.reviews_count}, {product.reviews_count_30_days_avg}, {product.reviews_count_180_days_avg},
                        {product.review_velocity}, {product.availability_of_amazon_offer}, {product.variations_count},
                        '{product.updated_at}'
                        );
                
        """
        frappe.db.sql(insert)
        frappe.db.commit()
        logger.debug(f"Product with asin {product.asin} inserted.")
    except IntegrityError as error:
        logger.error(str(error))
        frappe.db.rollback()
        update = f"""
        UPDATE product_compressed
                    SET title='{escape_string(product.title)}', root_category='{product.root_category}',
                    brand='{escape_string(product.brand)}',
                    url='{product.url}', count_on_amazon={product.count_on_amazon}, buy_box_90_days_avg={product.buy_box_90_days_avg},
                    new_offer_count_current={product.new_offer_count_current}, fba_fee={product.fba_fee},
                    referral_fee={product.referral_fee}, package_height={product.package_height}, package_width={product.package_width},
                    package_length={product.package_length}, package_weight={product.package_weight},
                    sales_rank_current={product.sales_rank_current}, reviews_rating={product.reviews_rating},
                    reviews_count={product.reviews_count}, reviews_count_30_days_avg={product.reviews_count_30_days_avg},
                    reviews_count_180_days_avg={product.reviews_count_180_days_avg}, review_velocity={product.review_velocity},
                    availability_of_amazon_offer={product.availability_of_amazon_offer}, variations_count={product.variations_count},
                    updated_at='{product.updated_at}'
                    WHERE asin='{product.asin}';
        """
        frappe.db.sql(update)
        frappe.db.commit()
        logger.debug(f"Product with asin {product.asin} updated.")


class FetchKeepaProductsCommand(Command):
    ASIN_LIMIT: int = 300
    
    def __init__(
            self,
            keepa_api_key: str,
            codes_list: list[str],
            saving_strategy: SavingStrategyProtocol,
            product_repository,
            product_codes_is_asin=True,
    ):
        """

        :param keepa_api_key:  API key from Keepa account https://keepa.com/#!api
        :param saving_strategy: Any object that include `save` method that consume list of strings
        :param codes_list: list of up to 100 ASIN codes str
        """
        self.keepa: Keepa = self.connect_to_keepa(keepa_api_key)
        self.saving_strategy: SavingStrategyProtocol = saving_strategy
        
        if len(codes_list) > self.ASIN_LIMIT:
            raise ValueError(f"Could not process more than {self.ASIN_LIMIT} asins.")
        
        self.code_list: list[str] = codes_list
        self.product_codes_is_asin = product_codes_is_asin

    def process_product_entry(self, product: dict):
        # print(product)
        asin = product.get('asin')
        if not asin:
            return
        upcs = product.get('upcList', [])

        if not upcs:
            logger.warning(f'No upc`s in product with ASIN: {asin}')
            upcs = []

        for upc in upcs:
            save_associative_pair(upc, asin)

        # convert np.array`s to list`s and dataframes to dictionaries inplace
        # convert_to_basic_types(product)
        buy_box_90_days_avg = product['stats_parsed'].get('avg90', {'BUY_BOX_SHIPPING': 0}).get('BUY_BOX_SHIPPING', 0)
        reviews_count_30_days_avg: int = product['stats_parsed'].get('avg30', {'COUNT_REVIEWS': 0}).get('COUNT_REVIEWS', 0)
        reviews_count_180_days_avg: int = product['stats_parsed'].get('avg180', {'COUNT_REVIEWS': 0}).get('COUNT_REVIEWS', 0)
        review_velocity: int = (reviews_count_30_days_avg - reviews_count_180_days_avg)

        compressed_product = KeepaProductModelDto(
            asin=asin,
            title=escape_string(product['title']),
            brand=product['brand'],
            url=f"https://amazon.com/dp/{asin}",
            count_on_amazon=find_pack_number(product['title']),
            buy_box_90_days_avg=buy_box_90_days_avg,
            new_offer_count_current=product['stats_parsed'].get('current', {'COUNT_NEW': 0}).get('COUNT_NEW', 0),
            fba_fee=product['fbaFees'].get('pickAndPackFee', 0) / 100 if product.get('fbaFees') else 0, # TODO I NEED TO CLARIFY WHETHER THE VALUE DOES NOT NEED TO BE DIVIDED BY 100
            referral_fee=Decimal(buy_box_90_days_avg) * Decimal('0.15'),
            package_height=product['packageHeight'] / 10 if product['packageHeight'] != -1 else 0,
            package_width=product['packageWidth'] / 10 if product['packageWidth'] != -1 else 0,
            package_length=product['packageLength'] / 10 if product['packageLength'] != -1 else 0,
            package_weight=product['packageWeight'] / 10 if product['packageWeight'] != -1 else 0,
            sales_rank_current=product['stats_parsed'].get('avg90', {'SALES': 0}).get('SALES', 0),
            reviews_rating=product['stats_parsed'].get('current', {'RATING': -1}).get('RATING', -1),
            reviews_count=product['stats_parsed'].get('current', {'COUNT_REVIEWS': 0}).get('COUNT_REVIEWS', 0),
            reviews_count_30_days_avg=product['stats_parsed'].get('avg30', {'COUNT_REVIEWS': 0}).get('COUNT_REVIEWS', 0),
            reviews_count_180_days_avg=product['stats_parsed'].get('avg180', {'COUNT_REVIEWS': 0}).get('COUNT_REVIEWS', 0),
            review_velocity=review_velocity,
            root_category=product['rootCategory'],
            availability_of_amazon_offer=product['stats_parsed'].get('totalOfferCount', -1),
            variations_count=len(product.get('variations')) if product.get('variations') else 0
        )

        save_product_entry(compressed_product)

        # self.saving_strategy.save_object(
        #     product,
        #     asin
        # )

    @retry
    def connect_to_keepa(self, api_key) -> Keepa:
        return Keepa(api_key)

    # @retry
    def keepa_query(self, *args, **kwargs):
        return self.keepa.query(*args, **kwargs)

    @retry
    def keepa_update_status(self):
        self.keepa.update_status()

    def execute(self):
        self.keepa_update_status()
        if self.keepa.tokens_left < self.ASIN_LIMIT:
            print("Token limit reached")
            raise ValueError("Token limit reached")

        products = self.keepa_query(
            self.code_list,
            days=90,
            stats=90,
            buybox=True,
            rating=True,
            product_code_is_asin=self.product_codes_is_asin
        )
        
        result = list(map(self.process_product_entry, products))
        return result