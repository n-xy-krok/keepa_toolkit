# Copyright (c) 2023, N_XY and contributors
# For license information, please see license.txt

from keepa_toolkit_v2.command.bsr_fetch_command import FetchBSRCommand
import frappe
from keepa_toolkit_v2.common.saving_strategies import GoogleCloudStorageSavingStrategy
from frappe.model.document import Document

from keepa import Keepa
from loguru import logger



def fetch_bsr_rates():
    
    bucket_name = 'test_backet_ik'
    file_path = 'bsr_files/'
    file_name = "BSR_"
    file_format = 'csv'
    serializer = 'pickle'
    cloud_credentials = {
        "type": "service_account",
        "project_id": "clever-environs-381513",
        "private_key_id": "3c79b99add8cf23f80c3fdb3a7b705f19f2e87e0",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+QnEBiwZ7hCFt\n+UeiRA178FxF+47ZRvjGQYoDiPwZatWfw5xXVPIQ1DrgRf33bbo7Fxz2vHRYIooy\ncUaCIXlxeayHLnDfrooruFALqHI3UxkO6E21V1XL0sIFAViTf6yoz5k0rgMLTWEL\nFtK1wjHJi8v64EIG/OQ0m8sfMo9AlDFI9NFJ/NsYJWR/VfsijZxTci7vq3fi5UMh\n4jYKvP/Dh6vjKzuNQNb699nkcJj+pE9RY4eRyP6fWSm+8ywXRq7uTP0uup+yjTOW\nQUyA31KaOjQ4s/DDOT0WaiFkT3DH9GNsh6GCAlo+ZiNyxH+yrodxD51hUq77uCyp\nXueo4Q2bAgMBAAECggEACUZ98eBQOZdQdecQOfXtXlxycyr3ZNpT6H5rdw6nk6mk\nrnw/0/YdMvrVv/dJkVV1HcpbrB+14FIWwKzLDi8Jl6Gh6pk2AYGl1MnQhTD6GH5h\nm1tx5mRSl3Q35zyQj+hKh5RPWMUe+t8T2xGKxjeoa7X4BMwIDAUqLvJlbajuWOyK\n+V7asnWb0PAStqws+MfWkV5ZOJYD1EIYVe57nEo4HJ9oWLE0ewSah2Jzz6e5BQJx\nFFSqIhh3KdEu+163d6VurkvWX19NFWyzS44PoAIj8xugSVL7hS28RgsC2RmZkhMS\ncC7EX/A5Sk2V/rg6t8PLJ6MzIzDxdpxCJz0zv7y7WQKBgQDyeofcErWahJQBJ7Jf\nkEZnnB8CvKnK0K2WtwOsw5ZRZVGSkGPUWOCCl6Wn/jxDPZkv5L26qK7acM8ONr4M\nXi/Y/2UxnxP41a0Xbm0QxAJIG2rlFsxUC2Mffqm6jRYyulHaGZm8rSMiXkqoxzpU\n4+VeOwSxxRjWTbfVJchxoQ5VrQKBgQDI3nbjUqgZ/hAsjp0lY+agjE3q0oRTta3P\ngpnTVPI/Pz3LHPiBjERSd0YUkJaDr/CYHHZ9SQtMJEFzOYx5MZsEG2ndaYmcJeME\nVoNz5mMYBECTFSSuF6VIej4ge10+uGkLqs9m0XVvR6OWh3ckenP+jMoyfV8NW5Mm\ncAI6+6+JZwKBgQCW90jM8O0WRgLr8S4yJ15eNaptBs9j0aayFkH4d1GIXd96s+Ej\nnS4ywH3H6RbV+mz8j5q8szciE2gmfli21JFPtjrnuRYzL73zv7Gx5YdSI5fbB+Wx\nUeGmAzNjTWILtUsu0g+Qk+8y/6fWCyw7HCR7FZ2nnIIJMxwIXdy9Ojv1hQKBgDsA\nLUekVcAcwj90PHBpIaryy0g4Qaoc6UrUf751axpsxwfMIKpcuwzGAA22Up+npDHp\n2lv+gpUA6UChHMPUcEYKj1P71fHJpx71EnOOrni1dwo84aNJSsE5Ntpvofn0TNai\ne30l9SqUjlZ+v941IbEZRaMC9FKGHXji2aTdnLftAoGBANvR1sY5/8iVgrCxv6ix\nBGOteXt/MNWhPyyrVLbZAMAnYVIPmWdaIxKyBC0ZhNfpjwuJvDSvjP0CTwjKS5rN\nihQfdkGn766xXCZMHB13zOtvoAyrNJhJ6uLdy9SXvGoukVPU/rqsgYu53LLhDAmJ\nYVGN5JkK5E10dnJ2GAsbQyrO\n-----END PRIVATE KEY-----\n",
        "client_email": "clever-environs-381513@appspot.gserviceaccount.com",
        "client_id": "111999371783688268963",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/clever-environs-381513%40appspot.gserviceaccount.com"
    }

    saving_strategy = GoogleCloudStorageSavingStrategy(
        bucket_name,
        file_name_prefix=file_name,
        list_file_format=file_format,
        list_file_path=file_path,
        serializer=serializer,
        single_file_format='pickle',
        single_file_path='files/',
        google_storage_credentials=cloud_credentials
    )
    
    
    queue_repository = frappe.get_single('Keepa Tools RabbitMQ Settings')
    queue_name = 'asins_queue'
    queue_repository.get_connection()
    
    
    keepa_settings = frappe.get_single('Keepa Settings')
    api_key = keepa_settings.get_password('keepa_api_key')
    
    fetch_bsr_settings = frappe.get_single('Category Retriever Job Settings')
    
    category_ids = [
        frappe.get_doc('Keepa Category Item', item.category_link).category_id
        for item in fetch_bsr_settings.category_select
    ]
    

    command = FetchBSRCommand(
        api_key,
        category_ids=category_ids,
        saving_strategy=saving_strategy,
        rank_limit=100_000,
        queue_name=queue_name,
        queue_repository=queue_repository
    )
    command.execute()

    
    
    
def retrieve_categegory(category_id: int | str):
    ...


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