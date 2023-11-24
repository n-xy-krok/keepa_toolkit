# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt

from keepa_toolkit_v2.command.bsr_fetch_command import FetchBSRCommand
import frappe
from keepa_toolkit_v2.common.saving_strategies import SavingStrategyMock
from frappe.model.document import Document

from keepa import Keepa
from loguru import logger



def fetch_bsr_rates():
    
    bucket_name = 'test_backet_ik'
    file_path = 'bsr_files/'
    file_name = "BSR_"
    file_format = 'csv'
    serializer = 'pickle'
    
    saving_strategy = SavingStrategyMock()
    
    
    keepa_settings = frappe.get_single('Keepa Settings')
    api_key = keepa_settings.get_password('keepa_api_key')
    
    fetch_bsr_settings = frappe.get_single('Category Retriever Job Settings')
    
    category_ids = [
        frappe.get_doc('Keepa Category Item', item.category_link).category_id
        for item in fetch_bsr_settings.category_select
    ]
    

    command = FetchBSRCommand(
        Keepa(api_key),
        category_ids=category_ids,
        saving_strategy=saving_strategy,
        rank_limit=100_000,
        queue_name="CategoryRetrieve",
    )
    command.execute()



class CategoryRetrieverJobSettings(Document):
    def __init__(self, *args, **kwargs):
        super(CategoryRetrieverJobSettings, self).__init__(*args, **kwargs)
        
    @frappe.whitelist()
    def fetch_root_categories(self):
        keepa_settings = frappe.get_single('Keepa Settings')
        
        keepa_api_key = keepa_settings.get_password('keepa_api_key')
        
        keepa_client = Keepa(keepa_api_key)
        
        categories = keepa_client.category_lookup(0)
        
        root_categories = {v['name']: k for k, v in categories.items()}
        
        for category_name, category_id in root_categories.items():
            exist: bool = frappe.db.exists({'doctype': "Keepa Category Item", "category_name": category_name, "category_id": category_id})
            if exist:
                continue
            category_item = frappe.get_doc(doctype='Keepa Category Item', category_name=category_name, category_id=category_id)
            category_item.save()
    
    def retrieve_categegory(self):
        fetch_bsr_rates()