# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt
from decimal import Decimal
import json

import os
from uuid import uuid4

from loguru import logger

import frappe
from frappe.model.document import Document

import pandas as pd

from keepa_toolkit_v2.models import KeepaProductModelDto
from keepa_toolkit_v2.common.utils import (
    calculate_prep_cost,
    divide_chunks,
)
from keepa_toolkit_v2.common.enums import PriorityEnum


logger = frappe.logger("tasks")




class PriceAnalysis(Document):
    @frappe.whitelist()
    def start_price_processing(self):
        
        if self.status == 'Success':
            frappe.msgprint("The analysis process has successfuly finished.")
            return
        
        if self.status != 'Initiated':
            frappe.msgprint("The analysis process has already begun.")
            return
        
        frappe.msgprint("Processing enqueued. Result will be displayed on Keepa Analysis Tools Page.")
        
        self.status = 'In progress'
        self.save()
        
        frappe.enqueue(self.price_processing, queue='long', job_name=f"{self.analysis_name} price analysis")
    
    def retrieve_upcs(self, upc_list):
        ...
        
    def enqueue_retrieving_delta(self, target_upcs):
        keepa_service = frappe.get_single("Keepa Settings")
        if not self.auto_retrieve_flag:
            return
        
        if not keepa_service.key_specified():
            logger.error("Keepa api key not provided. Data retrieving is not possible.")
            raise ValueError("Keepa api key not provided. Data retrieving is not possible.")
        
        chunks = divide_chunks(target_upcs, 100)
        
        
        for index, chunk in enumerate(chunks):
            new_doc = frappe.new_doc(doctype = "Keepa Retrieving Queue Item Holder")
            new_doc.name = f'{self.name}-{index}'

            new_doc.analysis_link = self.name
            # logger.debug(json.dumps(chunk))
            new_doc.item_list = json.dumps(chunk)
            new_doc.priority =  PriorityEnum.HIGH.value
            new_doc.item_type = 'UPC'
            try:
                new_doc.save()
            except frappe.exceptions.DuplicateEntryError:
                # WRITE HERE WHAT TO DO IF ENTRIES FOR THIS PRICE TABLE IS ALREADY ENQUEUED AND CURRENTLY WASN`T PROCESSED
                break   
    
    def get_asin_upc_relation(self, upcs) -> dict:
        results = frappe.db.sql(f"""
        SELECT upc, asin FROM upc_asin where upc in ({','.join([f"'{item}'" for item in upcs])});
        """)
        return {item[1]: item[0] for item in results}

    def price_processing(self):
        try:
            file_path = os.path.join(os.getcwd(), frappe.get_site_path()[2:]) + self.price_file

            price_df = pd.read_excel(file_path, self.sheet_name)

            upcs = price_df['upc'].astype(str).to_numpy()

            associates: dict[str, str] = self.get_asin_upc_relation(upcs)

            upcs_delta = set(upcs).difference(set(associates.values()))
            
            if upcs_delta:
                self.enqueue_retrieving_delta(upcs_delta)
                
            asins_bodies: dict = self.get_products_bodies(associates)

            new_result_doc = frappe.new_doc('Keepa Analysis Result')
            new_result_doc.analysis_doc = self.name
            new_result_doc.save()
            
            for asin, upc in associates.items():
                price_row = price_df.query(f"upc == {upc}").iloc[0]
                product_row: KeepaProductModelDto = asins_bodies.get(asin)
                if not product_row:
                    continue

                buy_box_90_days_avg = product_row.buy_box_90_days_avg

                new_entry = dict()
                new_entry['upc'] = upc
                new_entry['asin'] = asin
                new_entry['sku'] = price_row.SKU
                new_entry['brand'] = product_row.brand
                new_entry['title'] = product_row.title
                new_entry['name_in_price'] = price_row['Product Name']
                new_entry['description_in_price'] = price_row['Product Description']
                new_entry['url'] = f"https://amazon.com/dp/{asin}"
                new_entry['count_on_amazon'] = product_row.count_on_amazon
                new_entry['count_in_case_for_price'] = price_row['Count in case for price']
                new_entry['case_price'] = price_row['Price per case']
                new_entry['discount'] = price_row['Product Discount']
                new_entry['discount_changing_in_new_price'] = 0
                new_entry['price_changing_in_a_new_price'] = 0

                price_per_count_on_amazon = (Decimal(price_row['Price per case']) / price_row['Count in case for price']) * (
                            (100 - Decimal(price_row['Product Discount'])) / 100) * product_row.count_on_amazon
                new_entry['price_per_count_on_amazon'] = price_per_count_on_amazon
                new_entry['buy_box_90_days_avg'] = buy_box_90_days_avg
                new_entry['sales_psc']: int = 0  # not referred to any column in original table TODO
                new_entry['new_offer_count_current'] = product_row.new_offer_count_current
                new_entry['fba_fee'] = product_row.fba_fee
                new_entry['referral_fee'] = product_row.referral_fee

                new_entry['package_height'] = product_row.package_height
                new_entry['package_width'] = product_row.package_width
                new_entry['package_length'] = product_row.package_length
                new_entry['package_weight'] = product_row.package_weight

                volume_product_weight = Decimal(
                    new_entry['package_height'] * new_entry['package_width'] * new_entry['package_length']) / 5000
                new_entry['volume_product_weight'] = volume_product_weight

                payable_weight = Decimal(
                    volume_product_weight if volume_product_weight > new_entry['package_weight'] else new_entry[
                        'package_weight'])
                new_entry['payable_weight'] = payable_weight

                # TODO get data from prep settings source
                prep_to_amazon_delivery_cost = payable_weight * Decimal('2.205') * Decimal('0.36')
                new_entry['customer_to_prep_delivery'] = Decimal(
                    '0')  # if supplier -> prep delivery cost is free use 0 else use prep_amazon_delivery cost
                new_entry['prep_amazon_delivery'] = prep_to_amazon_delivery_cost
                new_entry['prep'] = calculate_prep_cost(product_row.count_on_amazon)

                payoneer_fee = Decimal(
                    price_per_count_on_amazon + new_entry['customer_to_prep_delivery'] + new_entry['prep']) * Decimal(
                    '0.01')
                new_entry['payoneer_fee'] = payoneer_fee

                # TODO get salesrank from inner db as default value for root category
                new_entry['sales_rank_current'] = product_row.sales_rank_current

                new_entry['reviews_rating'] = product_row.reviews_rating
                new_entry['reviews_count'] = product_row.reviews_count
                new_entry['reviews_count_30_days_avg'] = product_row.reviews_count_30_days_avg
                new_entry['reviews_count_180_days_avg'] = product_row.reviews_count_180_days_avg
                new_entry['review_velocity'] = product_row.review_velocity
                new_entry['category'] = product_row.root_category
                new_entry['availability_of_amazon_offer'] = product_row.availability_of_amazon_offer

                new_entry['total_cost_sellerboard'] = Decimal(
                    price_per_count_on_amazon + new_entry['customer_to_prep_delivery'] + new_entry['prep'] + payoneer_fee)
                total_cost_plus_inbound_fee = (
                        new_entry['total_cost_sellerboard'] +
                        new_entry['prep_amazon_delivery'] +
                        new_entry['fba_fee'] +
                        new_entry['referral_fee']
                )
                new_entry['total_cost_plus_inbound_fee'] = total_cost_plus_inbound_fee
                total_cost_plus_inbound = (
                        price_per_count_on_amazon +
                        new_entry['customer_to_prep_delivery'] +
                        new_entry['prep'] +
                        new_entry['prep_amazon_delivery'] +
                        payoneer_fee
                )
                new_entry['total_cost_plus_inbound'] = total_cost_plus_inbound
                new_entry['profit'] = buy_box_90_days_avg - total_cost_plus_inbound_fee
                new_entry['roi'] = Decimal(new_entry['profit'] / total_cost_plus_inbound)
                new_entry['variations'] = product_row.variations_count
                new_entry['map'] = price_row['MAP'] if price_row['MAP'] else 0
                new_entry['restr_sup'] = price_row['Restriction']
                new_entry['researcher_notes'] = ''
                new_entry['admin_notes'] = ''
                new_entry['product_approving'] = False
                new_entry['price_analysis_name'] = self.name

                new_result_item_doc = frappe.new_doc('Keepa Analysis Result Item', parent_doc=new_result_doc,
                                                    parentfield='items')
                new_result_item_doc.update(new_entry)

                try:
                    new_result_item_doc.save()
                except frappe.exceptions.UniqueValidationError as error:
                    self.status = 'Failure'
                    self.failure_reason = str(ex)
                    self.save()
                    raise error
                    
                new_result_doc.items.append(new_result_item_doc)
            
            frappe.db.commit()
            
            self.status = 'Success'
            self.save()
        except Exception as ex:
            self.failure_reason = str(ex)
            logger.error(str(ex))
            self.status = 'Failure'
            self.save()

        
    def get_products_bodies(self, associates: dict) -> dict[str, KeepaProductModelDto]:
        results = self.get_products_by_asins(asins_list=associates.keys())
        # product[2] is asin column from db
        result = {product[2]: KeepaProductModelDto.from_tuple(product) for product in results}
        return result
        # print(price_df)
        
    def get_products_by_asins(self, asins_list: list[str]):
        try:
            results = frappe.db.sql(
                f"""
                SELECT * FROM product_compressed
                WHERE asin in ({', '.join([f"'{item}'" for item in asins_list])});
                """
            )
            return results
        except Exception as ex:
            frappe.db.rollback()
            logger.error(str(ex))
    
    def get_asin_upc_relation(self, upcs) -> dict:
        results = frappe.db.sql(f"""
        SELECT upc, asin FROM upc_asin where upc in ({','.join([f"'{item}'" for item in upcs])});
        """)
        return {item[1]: item[0] for item in results}
