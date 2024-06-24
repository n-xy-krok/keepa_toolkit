# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt

from datetime import datetime, timedelta, tzinfo
import io
import json
from pprint import pprint
import sys

import pytz
import frappe
from frappe.model.document import Document

import pandas as pd


def remove_service_keys(target_dict: dict):
    service_keys = ['owner', 'creation','modified', 'modified_by', 'docstatus', 'idx', 'parenttype', 'doctype', 'parentfield', 'parent']
    for key in service_keys:
        if target_dict.get(key) is not None:
            del target_dict[key]
    return target_dict


@frappe.whitelist()
def export_to_csv(name, selected_rows):
    doc = frappe.get_doc('Keepa Analysis Result', name)
    selected_rows = json.loads(selected_rows)
    if  len(selected_rows) > 0:
        df = pd.json_normalize((remove_service_keys(item.as_dict()) for item in filter(lambda x: x.name in selected_rows, doc.items)))
    else:
        df = pd.json_normalize((remove_service_keys(item.as_dict()) for item in doc.items))      
    
    csv = df.to_csv()
    
    filename = 'export_' + name + '_' + datetime.utcnow().isoformat()[:19] + '.csv'
    new_file = frappe.new_doc('File')
    new_file.file_name = filename
    new_file.content = csv
    new_file.save()
    
    return filename
   
   
@frappe.whitelist()
def export_to_xlsx(name, selected_rows):
    doc = frappe.get_doc('Keepa Analysis Result', name)
    selected_rows = json.loads(selected_rows)
    
    if len(selected_rows) > 0:
        df = pd.json_normalize((remove_service_keys(item.as_dict()) for item in filter(lambda x: x.name in selected_rows, doc.items)))
    else:
        df = pd.json_normalize((remove_service_keys(item.as_dict()) for item in doc.items))
        
    buffer =  io.BytesIO()
    with pd.ExcelWriter(buffer) as fileobject:
        df.to_excel(fileobject)
    
    new_file = frappe.new_doc('File')
    filename = 'export_' + name + '_' + datetime.utcnow().isoformat()[:19] + '.xlsx'
    new_file.file_name = filename
    new_file.content = buffer.getvalue()
    new_file.save()

    return filename
    
def get_or_create_brand_name(brand_name):
    existed_brand = frappe.get_list('Brand', filters={'brand': brand_name})
    if existed_brand :
        brand = existed_brand[0]['name']
    else:
        brand = frappe.get_doc({'doctype': 'Brand', 'brand': brand_name})
        brand.save()
        brand = brand.brand
    
    return brand

def convert_to_sales_order_item(item_list, warehouse):
    result = []
    
    for item in item_list:
        body = {
                'delivery_date': datetime.now() + timedelta(days=1), 
                'item_code': item.item_code,
                'qty': 1,
                'rate': item.standard_rate,
                'warehouse': warehouse,
                'doctype': 'Purchase Order Item'
        }
        so_item = frappe.get_doc(body)
        result.append(so_item)
        
    
    
    return result
    
def create_items(price_analysis_items):    
    existed_items = frappe.get_list('Item', filters={'item_code': ['in', [item.asin for item in price_analysis_items]]})
    
    item_to_create = set([item.asin for item in price_analysis_items]) - set([item['name'] for item in existed_items])
    item_to_update = set([item['name'] for item in existed_items]).intersection(set([item.asin for item in price_analysis_items]) )
    
    item_to_create = [item for item in price_analysis_items if item.asin in item_to_create]
    item_to_update = [item for item in price_analysis_items if item.asin in item_to_update]
    
    
    items = []
    
    for raw_item in item_to_create:
        brand = get_or_create_brand_name(raw_item.brand)
            
        unit = 'Unit' if raw_item.count_in_case_for_price <= 1 else 'Set'
        
        body = {
            'doctype': 'Item',
            'brand': brand,
            'item_name': raw_item.title,
            'item_code': raw_item.asin,
            'item_group': 'Amazon Products',
            'stock_uom': unit,
            'max_discount': raw_item.discount,
            'standard_rate': raw_item.case_price,
        }
        item = frappe.get_doc(body)
        item.save()
        items.append(item)
    
    for raw_item in item_to_update:
        # TODO inmplement Item update
        items.append(frappe.get_doc('Item', raw_item.asin))
    # barcode = frappe.new_doc('Item Barcode')
    # barcode.save()
    # print(len(items))
    return items


@frappe.whitelist()
def create_order_from_selected(name, selected_rows, supplier, company, warehouse, currency, territory, conversion_rates):
    doc = frappe.get_doc('Keepa Analysis Result', name)
    selected_rows = json.loads(selected_rows)
    
    if len(selected_rows) > 0:
        items = (item for item in filter(lambda x: x.name in selected_rows, doc.items))
    else:
        raise Exception("Please select the products you would like to order")
    
    items = create_items(list(items))
    items = convert_to_sales_order_item(items, warehouse)
    
    new_order = frappe.get_doc(doctype='Purchase Order', **{
        'supplier': supplier,
        'transaction_date': datetime.now(),
        'schedule_date': datetime.now(),
        'company': company,
        'currency': currency,
        'territory': territory,
        'conversion_rate': conversion_rates,
        'status': 'Draft',
        'items': items,
    })
    # pprint(new_order.as_dict())
    new_order.save()
    return new_order.name


class KeepaAnalysisResult(Document):
    pass
    
