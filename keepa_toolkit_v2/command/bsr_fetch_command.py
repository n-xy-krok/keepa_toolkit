import datetime
from typing import Any

from keepa import Keepa
from loguru import logger

from keepa_toolkit_v2.common.saving_strategies import (

    SavingStrategyProtocol
)
from keepa_toolkit_v2.common.command_base import Command
# from queues.rabbitmq_repository import get_rabbit_repository


class FetchBSRCommand(Command):

    def __init__(self, keepa_api_key: str, category_ids: list[int], saving_strategy: SavingStrategyProtocol, rank_limit: int, queue_name: str = None, queue_repository = None):
        """
        This command fetching best sale rank from keepa api and store it by specified strategy.
        :param keepa_api_key: API key from Keepa account https://keepa.com/#!api
        :param category_ids: list of integer category identifier from Amazon. You can get categories here https://keepa.com/#!categorytree
        :param saving_strategy: Any object that include `save` method that consume list of strings
        :param rank_limit: limit of resulting count of entries
        :param queue_name: Specify a queue name to add BSR records to the queue for processing
        """
        self.queue_repository = queue_repository
        self.queue_name = queue_name
        self.keepa = Keepa(keepa_api_key)
        self.category_ids = category_ids
        self.saving_strategy = saving_strategy
        self.rank_limit = rank_limit

    def execute(self):
        logger.info(f"Started to fetch {self.rank_limit} asins from each of {self.category_ids} categories.")
        request_cost = 50
        self.keepa.update_status()
        if self.keepa.tokens_left < request_cost * len(self.category_ids):
            logger.debug(f'Not enough token to fetch BSR from keepa. '
                         f'Tokens needed: {request_cost * len(self.category_ids)}, tokens left: {self.keepa.tokens_left}.')
            return
        best_seller_rates = []
        for category_id in self.category_ids:
            bsr = self.keepa.best_sellers_query(category=category_id)
            bsr = bsr[:self.rank_limit]

            filename = self.saving_strategy.save_list(bsr, name=f"{str(category_id)}_{int(datetime.datetime.utcnow().timestamp())}")

            best_seller_rates.append(bsr)

        if self.queue_name and self.queue_repository:
            logger.debug(f"Start to insert {self.rank_limit * len(self.category_ids)} entries to a processing queue")

            # add entries from different categories to a queue with collection rotations
            for asins in zip(*best_seller_rates):
                for asin in asins:
                    self.queue_repository.push_queue_entry('task_queue', asin)
            logger.debug(f"BSR entries successfully pushed to a processing queue")


# def run_test_local_storage():
#     saving_strategy = LocalFileSavingStrategy('files/test', 'csv')
#     command = FetchBSRCommand(
#         settings.keepa_api_key,
#         category_id=3760911,
#         saving_strategy=saving_strategy,
#         rank_limit=100_000
#     )
#     command.execute()


# def run_test_firestore_storage():
#     category_id = 3760911
#     collection_name = f"BSR_{category_id}_{int(datetime.datetime.utcnow().timestamp())}"
#     firestore_credentials = {
#         "type": "service_account",
#         "project_id": "teststepify",
#         "private_key_id": "c171367406bd941c11ec0147ed40679b615c3bbb",
#         "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCw2WnJXOY/WxlM\nWOb46II4N5O0XOnuCfXHWqd7Ff+tcqd6fonUGcSjrt9gYFmn4wTbA0DVE4Da0YQ7\nwNuRCE9Uc7wO5nizt8TdG6DqpzTykJtF78wgOio5mY+vYgIgychlDqEUz4xZibIa\nQGD0ySfv2Z/Gdbov2PqduAkZlBBsSF2VeSnVIwMP6Tls+RqUqBdwonUykajCx2Js\nKGNb6kfitr8JxI9nJL29Sa2/azZPPQ0RYOxiy4HKasSfQA5SGliefOMlP6Vl+mg1\n3qwEmF/JigWjJ7N2jEM0o/CVzGBiO10pekyD1hBa8bQ9mNuB8Ek+BZW0RMABHLxx\nHVIu/wU9AgMBAAECggEAFJUhOkXdKFeGcLvSGfsZmMhjIPVFZqZzHAIhhvJPARVc\nh6Wtjel32OMT7WgZVzBVUtYkmAf+kbkbzkaKZ21Tqk0UAJnzR1tmhqNeDaFrSKXq\n5G9dzwxl+LE2J1/dRoqlj5ltxvnG6G7dsy91+VdmFgS6k0MFaeczd+LA8GCqrqdl\n8IHgmYUVAArpH62CjF0G6IgPJuruf4vxP0XIzvsFWqxwEmY4yoD+1703vZwrv7JE\nYdsBthFhJdxXaf6cpIwX04RBdhDU2rNYdsJV793UuRkD4MIVu2AF4IivL8MuxODz\nvn7oDfxOKxU4Kge525rT8hAG5X1SN522jRCDPpzrMQKBgQDXrF7+f5KrWVu0fSUh\ngPYHvnBOhObkWHdR8gYEZRzE8gu+2YIWaN+OVJ5vWmgd6Q7boqunDdX510S7Gu8X\nm/nIo50wdHX8gr5hGMAy2neTTJxk8NZm1d+qCMcifPZQmwPKzc63+utvl2uXc81x\nHFipwu8JM/qX4KVvZOg5soN7CQKBgQDR6qb6b83HUmeN4NrDB0INoUgHIVJQve+2\na8SxzgvpA8WsxhLw/UVB6S3A00TMldbLAvVj57FGIAoFE0anxDc0asTzALlzmw0R\nZbA/ILRr7ik4H2GLRh0CIxEn1DAcBstYLcSiFL5jGI8VCy21Ow/gba5BAEpvYfdS\nDlYlBIphlQKBgQCGL1mfEDJp55vV7PLYKItqgAMR50Bcm+oJEwLJYzuGEW5bXKrC\nVJeaz20PrPkQevtijZAszL/vxQ6fNv/A+atsONfI2Py7kYDvml2ihihVfhnj96/9\npV2WRgXXoFYECp5OZMQT1cr70AMB5OvSTyee34inei6UphFoACk4FKgXOQKBgBM3\nBofgGhcHvQDotz2o+Wvj+oLkkvNfH4U0QjIAaWiv9rVFAFDc7i0FWjHPZPnRMXMt\n1yaI+9oubpxeUlZjCKacq2CPDWq+o6iXBVYR+VHz4AQKI1SrW1ZpvVVHAAxgttZg\ngiJQAclyYw6LEmkHegGSKKQ8kZO2hPwW4d3Ll5stAoGAamt7qITTed2cqUVqdpBr\nzOpnhjl0qEiSQxgUSWGqG5vnSz3E/Md48H0cD59D/Oq+0la69HBg/HxU6Ce+70HR\nNRrUCoT740MXczGBi5lSG40sgVPuZIDEsEUAL3ykTpo5CC5FHpL0vf/cH5yk6xvQ\nEW5l4CgmZWEMsTOIvzhUgaE=\n-----END PRIVATE KEY-----\n",
#         "client_email": "firebase-adminsdk-vbt5k@teststepify.iam.gserviceaccount.com",
#         "client_id": "102259574348480203886",
#         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#         "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-vbt5k%40teststepify.iam.gserviceaccount.com"
#     }
#     saving_strategy = FireStoreSavingStrategy(collection_name, firestore_credentials)
#     command = FetchBSRCommand(
#         settings.keepa_api_key,
#         category_id=category_id,
#         saving_strategy=saving_strategy,
#         rank_limit=100_000
#     )
#     command.execute()


# def run_test_google_cloud_storage(category_ids: list[int] = [3760911, 3760901]):
#     category_ids = [3760911, 3760901]
#     bucket_name = 'test_backet_ik'
#     file_path = 'bsr_files/'
#     file_name = "BSR_"
#     file_format = 'csv'
#     serializer = 'pickle'
#     cloud_credentials = {
#         "type": "service_account",
#         "project_id": "clever-environs-381513",
#         "private_key_id": "3c79b99add8cf23f80c3fdb3a7b705f19f2e87e0",
#         "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+QnEBiwZ7hCFt\n+UeiRA178FxF+47ZRvjGQYoDiPwZatWfw5xXVPIQ1DrgRf33bbo7Fxz2vHRYIooy\ncUaCIXlxeayHLnDfrooruFALqHI3UxkO6E21V1XL0sIFAViTf6yoz5k0rgMLTWEL\nFtK1wjHJi8v64EIG/OQ0m8sfMo9AlDFI9NFJ/NsYJWR/VfsijZxTci7vq3fi5UMh\n4jYKvP/Dh6vjKzuNQNb699nkcJj+pE9RY4eRyP6fWSm+8ywXRq7uTP0uup+yjTOW\nQUyA31KaOjQ4s/DDOT0WaiFkT3DH9GNsh6GCAlo+ZiNyxH+yrodxD51hUq77uCyp\nXueo4Q2bAgMBAAECggEACUZ98eBQOZdQdecQOfXtXlxycyr3ZNpT6H5rdw6nk6mk\nrnw/0/YdMvrVv/dJkVV1HcpbrB+14FIWwKzLDi8Jl6Gh6pk2AYGl1MnQhTD6GH5h\nm1tx5mRSl3Q35zyQj+hKh5RPWMUe+t8T2xGKxjeoa7X4BMwIDAUqLvJlbajuWOyK\n+V7asnWb0PAStqws+MfWkV5ZOJYD1EIYVe57nEo4HJ9oWLE0ewSah2Jzz6e5BQJx\nFFSqIhh3KdEu+163d6VurkvWX19NFWyzS44PoAIj8xugSVL7hS28RgsC2RmZkhMS\ncC7EX/A5Sk2V/rg6t8PLJ6MzIzDxdpxCJz0zv7y7WQKBgQDyeofcErWahJQBJ7Jf\nkEZnnB8CvKnK0K2WtwOsw5ZRZVGSkGPUWOCCl6Wn/jxDPZkv5L26qK7acM8ONr4M\nXi/Y/2UxnxP41a0Xbm0QxAJIG2rlFsxUC2Mffqm6jRYyulHaGZm8rSMiXkqoxzpU\n4+VeOwSxxRjWTbfVJchxoQ5VrQKBgQDI3nbjUqgZ/hAsjp0lY+agjE3q0oRTta3P\ngpnTVPI/Pz3LHPiBjERSd0YUkJaDr/CYHHZ9SQtMJEFzOYx5MZsEG2ndaYmcJeME\nVoNz5mMYBECTFSSuF6VIej4ge10+uGkLqs9m0XVvR6OWh3ckenP+jMoyfV8NW5Mm\ncAI6+6+JZwKBgQCW90jM8O0WRgLr8S4yJ15eNaptBs9j0aayFkH4d1GIXd96s+Ej\nnS4ywH3H6RbV+mz8j5q8szciE2gmfli21JFPtjrnuRYzL73zv7Gx5YdSI5fbB+Wx\nUeGmAzNjTWILtUsu0g+Qk+8y/6fWCyw7HCR7FZ2nnIIJMxwIXdy9Ojv1hQKBgDsA\nLUekVcAcwj90PHBpIaryy0g4Qaoc6UrUf751axpsxwfMIKpcuwzGAA22Up+npDHp\n2lv+gpUA6UChHMPUcEYKj1P71fHJpx71EnOOrni1dwo84aNJSsE5Ntpvofn0TNai\ne30l9SqUjlZ+v941IbEZRaMC9FKGHXji2aTdnLftAoGBANvR1sY5/8iVgrCxv6ix\nBGOteXt/MNWhPyyrVLbZAMAnYVIPmWdaIxKyBC0ZhNfpjwuJvDSvjP0CTwjKS5rN\nihQfdkGn766xXCZMHB13zOtvoAyrNJhJ6uLdy9SXvGoukVPU/rqsgYu53LLhDAmJ\nYVGN5JkK5E10dnJ2GAsbQyrO\n-----END PRIVATE KEY-----\n",
#         "client_email": "clever-environs-381513@appspot.gserviceaccount.com",
#         "client_id": "111999371783688268963",
#         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#         "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/clever-environs-381513%40appspot.gserviceaccount.com"
#     }

#     saving_strategy = GoogleCloudStorageSavingStrategy(
#         bucket_name,
#         file_name_prefix=file_name,
#         list_file_format=file_format,
#         list_file_path=file_path,
#         serializer=serializer,
#         single_file_format='pickle',
#         single_file_path='files/',
#         google_storage_credentials=cloud_credentials
#     )

#     queue_repository = get_rabbit_repository()
#     queue_name = 'asins_queue'

#     command = FetchBSRCommand(
#         settings.keepa_api_key,
#         category_ids=category_ids,
#         saving_strategy=saving_strategy,
#         rank_limit=100_000,
#         queue_name=queue_name,
#         queue_repository=queue_repository
#     )
#     command.execute()



