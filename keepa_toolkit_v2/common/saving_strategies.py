import json
import pickle
from abc import ABC, abstractmethod
from typing import Protocol

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage
from google.oauth2 import service_account
from loguru import logger
from functools import partial

safety_json_serializer = partial(json.dumps, default=str)

allowed_serializers = {'json': safety_json_serializer, "pickle": pickle.dumps}


class SavingStrategyProtocol(Protocol):
    def save_list(self, data, name):
        ...

    def save_object(self, data, name):
        ...


class SavingStrategyBase(ABC):
    @abstractmethod
    def save_list(self, data, name):
        ...

    @abstractmethod
    def save_object(self, data, name):
        ...
        
class SavingStrategyMock(SavingStrategyBase):
    def save_list(self, data, name):
        ...

    def save_object(self, data, name):
        ...


class LocalFileSavingStrategy(SavingStrategyBase):
    def __init__(self, file_name: str, file_format: str = 'csv'):
        """
        Saves data to a local file as comma-separated records
        :param file_name: string value used as file name
        :param file_format: A string value to be used as the file format after the dot. By default, csv
        """
        self.file_name = f'{file_name}.{file_format}'

    def save_object(self, data: dict, name: str = None):
        with open(name if name else self.file_name, "w") as file:
            file.write(json.dumps(data, default=str))
        return self.file_name

    def save_list(self, data: list[str], name: str = None):
        with open(self.file_name, "w") as file:
            file.write(','.join(data))
        return self.file_name


class FireStoreSavingStrategy(SavingStrategyBase):
    def __init__(self, collection_name: str, firestore_credentials: dict, chunk_size: int = 20_000):
        cred = credentials.Certificate(firestore_credentials)
        try:
            app = firebase_admin.initialize_app(cred)
        except:
            app = firebase_admin.get_app()
        self.db_object = firestore.client(app)
        self.collection_name = collection_name
        self.chunk_size = chunk_size

    def save_object(self, data: dict, name: str = "default_name"):
        raise NotImplemented()

    def save_list(self, data: list[str], name: str = None):
        collection_ref = self.db_object.collection(self.collection_name)
        for chunk_num in range(0, len(data) // self.chunk_size):
            document = collection_ref.document(f'chunk_{chunk_num}')
            document_data = {}
            for rank, asin in enumerate(data[chunk_num: chunk_num + 1 * self.chunk_size]):
                document_data[str(rank + self.chunk_size * chunk_num)] = asin
            document.set(document_data)

        logger.info(f"Successfully saved {len(data)} entries")
        return self.collection_name


class GoogleCloudStorageSavingStrategy(SavingStrategyBase):
    def __init__(
            self, bucket_name: str,
            file_name_prefix: str,
            list_file_format: str,
            list_file_path: str,
            single_file_format: str,
            single_file_path: str,
            google_storage_credentials: dict,
            serializer: str = "pickle"
    ):
        self.bucket_name = bucket_name
        self.file_name_prefix = file_name_prefix
        self.list_file_path = list_file_path
        self.list_file_format = list_file_format
        self.file_name = f"{list_file_path}{file_name_prefix}.{list_file_format}"
        self.single_file_format = single_file_format
        self.single_file_path = single_file_path
        self.serializer = allowed_serializers[serializer]
        crd = service_account.Credentials.from_service_account_info(google_storage_credentials)
        self.file_storage_object = storage.Client(credentials=crd)

    def save_object(self, data: dict, name: str = None):
        bucket = self.file_storage_object.get_bucket(self.bucket_name)

        serialized = self.serializer(data)

        file_name = f"{self.single_file_path}{name if name else self.file_name}.{self.single_file_format}"
        blob = bucket.blob(file_name)
        blob.upload_from_string(serialized)

        logger.debug(f"Successfully saved object with filename {name if name else self.file_name} to "
                     f"Google Cloud Storage into {self.bucket_name} bucket.")
        return self.file_name

    def save_list(self, data: list[str], name: str = None):
        bucket = self.file_storage_object.get_bucket(self.bucket_name)

        serialized = ','.join(data)

        file_name = self.file_name if not name else f"{self.list_file_path}{self.file_name_prefix}{name}.{self.list_file_format}"

        blob = bucket.blob(file_name)
        blob.upload_from_string(serialized)

        logger.debug(f"Successfully saved BSR to {file_name} file in"
                     f" Google Cloud Storage into {self.bucket_name} bucket.")
        return self.file_name

