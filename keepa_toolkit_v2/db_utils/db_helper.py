from loguru import logger

import frappe
from frappe.model.document import Document
from keepa_toolkit_v2.common.utils import escape_string
import pymysql


@frappe.whitelist()
def get_association_count() -> dict:
    response = {'value': 0, "fieldtype": "Int"}
    try:
        query = "SELECT COUNT(*) FROM upc_asin;"
        result = frappe.db.sql(query)
        response['value'] = result[0][0]
        return response
    except Exception:
        return response
    

@frappe.whitelist()
def get_product_entry_count() -> dict:
    response = {'value': 0, "fieldtype": "Int"}
    
    try:
        query = "SELECT COUNT(*) FROM product_compressed;"
        result = frappe.db.sql(query)
        response['value'] = result[0][0]
        return response
    except Exception:
        return response

def save_associative_pair(upc, asin):
        
    # TODO REWRITE IT USING TEMPORARY TABLES AND MERGE COMMAND
    try:
        frappe.db.sql(
            f"""
            INSERT INTO upc_asin(upc, asin)
                VALUES ('{upc}', '{asin}');
            """
        )
        frappe.db.commit()
        
    except pymysql.err.IntegrityError:
        frappe.db.rollback()
        logger.debug(f"Entry like {upc} -> {asin} already exist.")
    except Exception:
        frappe.db.rollback()
        logger.debug(f"Entry like {upc} -> {asin} can not be saved")

def save_product_entry(product):
    try:
        frappe.db.sql(f"""
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
        """)
        logger.debug(f"Product with asin {product.asin} saved into db.")
        frappe.db.commit()
    except pymysql.err.IntegrityError as error:
        logger.error(str(error))
        frappe.db.rollback()
        frappe.db.sql(
            f"""
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
        )
        frappe.db.commit()
        
        logger.debug(f"Product with asin {product.asin} updated.")

def get_asin_upc_relation(upcs) -> dict:
    results: tuple[tuple[str, str]] = frappe.db.sql(f"""
    SELECT upc, asin FROM upc_asin where upc in ({','.join([f"'{item}'" for item in upcs])});
    """)
    return {item[1]: item[0] for item in results}

def get_products_by_asins(asins_list: list[str]):
        
    try:
        results: tuple[tuple] = frappe.db.sql(
            f"""
            SELECT * FROM product_compressed
            WHERE asin in ({', '.join([f"'{item}'" for item in asins_list])});
            """
        )
        return results
    except Exception as ex:
        frappe.db.rollback()
        logger.error(str(ex))
        return tuple()
