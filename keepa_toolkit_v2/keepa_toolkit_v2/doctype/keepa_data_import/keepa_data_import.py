# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt
from decimal import Decimal
import os


import frappe
from frappe.model.document import Document
from loguru import logger
import pandas as pd
import numpy

from keepa import Keepa

from keepa_toolkit_v2.common.utils import escape_string, find_pack_number
from keepa_toolkit_v2.models import KeepaProductModelDto

from keepa_toolkit_v2.db_utils.db_helper import save_associative_pair, save_product_entry


def save_upc_asin_associates(target_dataframe: pd.DataFrame) -> None:
    def save_associates(entry):
        if isinstance(entry['Product Codes: UPC'], str):
            upcs = [item.strip() for item in entry['Product Codes: UPC'].split(',')]
        else:
            return
        if not upcs:
            return
        for upc in upcs:
            save_associative_pair(upc, entry['ASIN'])

    target_dataframe.apply(save_associates, axis=1)


def save_product_compressed_data(target_dataframe: pd.DataFrame, keepa: Keepa) -> None:

    categories_tree: dict = keepa.category_lookup(0)
    categories = {category['name']: category_id for category_id, category in categories_tree.items()}
    columns = set(target_dataframe)
    etha = {
        'Title',
        'Brand',
        'ASIN',
        'URL: Amazon',
        'Package: Height (cm)',
        'Package: Width (cm)',
        'Package: Length (cm)',
        'Package: Weight (g)',
        'FBA Fees:',
        'Buy Box: 90 days avg.',
        'Reviews: Review Count - 30 days avg.',
        'Reviews: Review Count - 180 days avg.',
        'Sales Rank: 90 days avg.',
        'Reviews: Rating',
        'Reviews: Review Count',
        'Count of retrieved live offers: New, FBA',
        'New Offer Count: Current',
        'Categories: Root'
    }
    delta = etha.difference(columns)
    if delta:
        raise ValueError(f"The document has invalid header. Missed following columns: {', '.join(delta)}")
    
    def save_product_data(entry):
        try:
            buybox_90_avg = Decimal(entry['Buy Box: 90 days avg.']) if not pd.isnull(entry['Buy Box: 90 days avg.']) else Decimal('0')
        except Exception:
            buybox_90_avg = Decimal('0')

        reviews_count_30_days_avg: int = int(0 if numpy.isnan(entry['Reviews: Review Count - 30 days avg.']) else entry['Reviews: Review Count - 30 days avg.'])
        reviews_count_180_days_avg: int = int(0 if numpy.isnan(entry['Reviews: Review Count - 180 days avg.']) else entry['Reviews: Review Count - 180 days avg.'])
        review_velocity: int = (reviews_count_30_days_avg - reviews_count_180_days_avg)

        offer_count = -1 if numpy.isnan(entry['Count of retrieved live offers: New, FBA']) else entry['Count of retrieved live offers: New, FBA']

        product = KeepaProductModelDto(
            asin=entry.ASIN,
            title=escape_string(entry.Title),
            brand=str(entry.Brand),
            url=entry['URL: Amazon'],
            count_on_amazon=find_pack_number(entry.Title),
            buy_box_90_days_avg=buybox_90_avg,
            new_offer_count_current=0 if numpy.isnan(entry['New Offer Count: Current']) else entry['New Offer Count: Current'],
            fba_fee=-1 if numpy.isnan(entry['FBA Fees:']) else entry['FBA Fees:'],
            # TODO I NEED TO CLARIFY WHETHER THE VALUE DOES NOT NEED TO BE DIVIDED BY 100
            referral_fee=buybox_90_avg * Decimal('0.15'),
            package_height=entry['Package: Height (cm)'] if not numpy.isnan(entry['Package: Height (cm)']) else 0,
            package_width=entry['Package: Width (cm)'] if not numpy.isnan(entry['Package: Width (cm)']) else 0,
            package_length=entry['Package: Length (cm)'] if not numpy.isnan(entry['Package: Length (cm)']) else 0,
            package_weight=entry['Package: Weight (g)'] if not numpy.isnan(entry['Package: Weight (g)']) else 0,
            sales_rank_current=0 if numpy.isnan(entry['Sales Rank: 90 days avg.']) else entry['Sales Rank: 90 days avg.'],
            reviews_rating=int(-1 if numpy.isnan(entry['Reviews: Rating']) else entry['Reviews: Rating']),
            reviews_count=int(-1 if numpy.isnan(entry['Reviews: Review Count']) else entry['Reviews: Review Count']),
            reviews_count_30_days_avg=reviews_count_30_days_avg,
            reviews_count_180_days_avg=reviews_count_180_days_avg,
            review_velocity=review_velocity,
            root_category=categories.get(entry['Categories: Root'], 0),
            availability_of_amazon_offer=offer_count,
            variations_count=0
        )

        save_product_entry(product)

    target_dataframe.apply(save_product_data, axis=1)


class KeepaDataImport(Document):
    
    @frappe.whitelist()
    def process_exported_files(self):
        if not self.file:
            self.failure_reason = "The file is required"
            self.status = 'Failed'
            self.save()
            return
        
        if not self.file.endswith('.xlsx'):
            self.failure_reason = "Only .xlsx format supported"
            self.status = 'Failed'
            self.save()
            return
        
        if self.status == 'Success':
            frappe.msgprint("Data import successfuly finished.")
            return
        
        if self.status != 'Initiated':
            frappe.msgprint("Data import has already begun.")
            return
        
        frappe.msgprint("Processing enqueued. Result will be displayed on Keepa Analysis Tools Page.")
        
        self.status = 'In progress'
        self.save()
        
        frappe.enqueue(self.job, queue='long', job_name=f"{self.file} import")
    
    
    @frappe.whitelist()
    def job(self):        
		
        file_path = os.path.join(os.getcwd(), frappe.get_site_path()[2:]) + self.file
    
        keepa_settings = frappe.get_single("Keepa Settings")
        keepa = Keepa(keepa_settings.get_password("keepa_api_key"))
        
        logger.info(f"{file_path} started to process")

        df = pd.read_excel(file_path, sheet_name=self.sheet_name)
        try:
            save_upc_asin_associates(df)
            save_product_compressed_data(df, keepa=keepa)
            
            self.status = 'Success'
            self.successfully_imported = len(df)
            self.save()
            
            frappe.msgprint(f"Success. {len(df)} entries imported.")
        except (AttributeError, KeyError, ValueError) as error:
            logger.debug(self.file)
            logger.debug(str(error))
            self.failure_reason = str(error)
            self.status = 'Failed'
            self.save()
            
            
