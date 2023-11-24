# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt

from datetime import datetime, timedelta
import json
import frappe
from frappe.model.document import Document

from keepa_toolkit_v2.common.enums import PriorityEnum
from keepa_toolkit_v2.common.utils import *
from keepa_toolkit_v2.command.fetch_keepa_product_command import FetchKeepaProductsCommand
from keepa_toolkit_v2.common.saving_strategies import GoogleCloudStorageSavingStrategy

from loguru import logger


def wraper(func):
    def wraped(*args, **kwargs):
        try:
            print('started')
            return func(*args, **kwargs)
        except Exception as ex:
            logger = frappe.logger("tasks", allow_site=True, file_count=50)
            logger.error(ex)        
            print(str(ex))
            raise ex
    return wraped

def compile_and_run_command(item_list, api_key, item_code_is_asin: bool = True):
    file_name = f"default_name"
    file_format = 'csv'
    cloud_credentials = {
        "type": "service_account",
        "project_id": "clever-environs-381513",
        "private_key_id": "",
        "private_key": "",
        "client_email": "clever-environs-381513@appspot.gserviceaccount.com",
        "client_id": "111999371783688268963",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/clever-environs-381513%40appspot.gserviceaccount.com"
    }

    saving_strategy = GoogleCloudStorageSavingStrategy(
        bucket_name='test_backet_ik',
        file_name_prefix=file_name,
        list_file_path='',
        list_file_format=file_format,
        single_file_path='files/',
        single_file_format='pickle',
        serializer='pickle',
        google_storage_credentials=cloud_credentials
    )
    
    command = FetchKeepaProductsCommand(
        api_key,
        json.loads(item_list),
        saving_strategy,
        None,
        product_codes_is_asin=item_code_is_asin
    )
    try:
        command.execute()
    except ValueError as error:
        logger.error(str(error))

# @wraper
def keepa_product_retriever():    
    keepa_settings = frappe.get_single('Keepa Settings')
    if not keepa_settings.key_specified():
        raise ValueError("API key for Keepa is not specified.")
   
    
    high_priority: list = frappe.get_all('Keepa Retrieving Queue Item Holder', filters={'priority': PriorityEnum.HIGH.value})
    for item in high_priority:
        doc = frappe.get_doc('Keepa Retrieving Queue Item Holder', item.get('name'))
        compile_and_run_command(doc.item_list, keepa_settings.get_api_key(), item_code_is_asin=doc.item_type == 'ASIN')
        
        doc.delete()
        logger.debug(f"Document with name {item.get('name')} processed")
        return
    
    
    default_priority: list = frappe.get_list('Keepa Retrieving Queue Item Holder', filters={'priority': PriorityEnum.DEFAULT.value})
    for item in default_priority:
        doc = frappe.get_doc('Keepa Retrieving Queue Item Holder', item.get('name'))
        compile_and_run_command(doc.item_list, keepa_settings.get_api_key(), item_code_is_asin=doc.item_type == 'ASIN')
        
        doc.delete()
        logger.debug(f"Document with name {item.get('name')} processed")
        return
 
    low_priority: list = frappe.get_list('Keepa Retrieving Queue Item Holder', filters={'priority': PriorityEnum.LOW.value})
    for item in low_priority:
        doc = frappe.get_doc('Keepa Retrieving Queue Item Holder', item.get('name'))
        compile_and_run_command(doc.item_list, keepa_settings.get_api_key(), item_code_is_asin=doc.item_type == 'ASIN')
        
        doc.delete()
        logger.debug(f"Document with name {item.get('name')} processed")
        return

def is_current_time_between(from_time, to_time):
    from_time = datetime.strptime(from_time, '%H:%M:%S')
    to_time = datetime.strptime(to_time, '%H:%M:%S')

    current_time = datetime.now().replace(year=1900, month=1, day=1)

    if from_time > to_time:
        if current_time < to_time:
            current_time += timedelta(days=1)
            
        to_time += timedelta(days=1)

    return from_time <= current_time <= to_time

def keepa_category_retriever():
    category_retrieve_doc = frappe.get_single(doctype="Category Retriever Job Settings")
    
    tasks_item_in_queue = frappe.db.count("Keepa Retrieving Queue Item Holder")
        
    if not is_current_time_between(category_retrieve_doc.from_time, category_retrieve_doc.to_time):
        logger.info("task skiped in order of time settings")
        return
    
    if tasks_item_in_queue <= 0:
        logger.info("keepa category retrieving started")
        category_retrieve_doc.retrieve_categegory()
        return
    
    logger.info("task skiped in order of other task existing")
    
    