# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

from keepa import Keepa


class KeepaSettings(Document):
    def validate(self):        
        self.fetch_root_categories()
        
    def fetch_root_categories(self):
        
        keepa_api_key: str = self.get_password('keepa_api_key')
        
        keepa_client: Keepa = Keepa(keepa_api_key)
        
        categories = keepa_client.category_lookup(0)
        
        root_categories: dict[str, int] = {v['name']: k for k, v in categories.items()}
        
        for category_name, category_id in root_categories.items():
            exist: bool = frappe.db.exists({'doctype': "Keepa Category Item", "category_name": category_name, "category_id": category_id})
            if exist:
                continue
            category_item = frappe.get_doc(doctype='Keepa Category Item', category_name=category_name, category_id=category_id)
            category_item.save()
            
    def key_specified(self) -> bool:
        return bool(self.get_password('keepa_api_key'))
    
    def get_api_key(self) -> str:
        return self.get_password('keepa_api_key')

