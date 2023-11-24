import datetime
from typing import Any
import json

from numpy import insert

import frappe

from keepa import Keepa
from loguru import logger

from keepa_toolkit_v2.common.command_base import Command
from keepa_toolkit_v2.common.enums import PriorityEnum

from keepa_toolkit_v2.common.saving_strategies import (

    SavingStrategyProtocol
)
from keepa_toolkit_v2.common.utils import divide_chunks

# from queues.rabbitmq_repository import get_rabbit_repository


class FetchBSRCommand(Command):

    def __init__(self, keepa_object: Keepa, category_ids: list[int], saving_strategy: SavingStrategyProtocol, rank_limit: int, queue_name: str = "default"):
        """
        This command fetching best sale rank from keepa api and store it by specified strategy.
        :param keepa_api_key: API key from Keepa account https://keepa.com/#!api
        :param category_ids: list of integer category identifier from Amazon. You can get categories here https://keepa.com/#!categorytree
        :param saving_strategy: Any object that include `save` method that consume list of strings
        :param rank_limit: limit of resulting count of entries
        :param queue_name: Specify a queue name to add BSR records to the queue for processing
        """
        self.name = queue_name
        self.keepa = keepa_object
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
        
        best_seller_rates = set()
        for category_id in self.category_ids:
            bsr = self.keepa.best_sellers_query(category=category_id)
            bsr = bsr[:self.rank_limit]

            # filename = self.saving_strategy.save_list(bsr, name=f"{str(category_id)}_{int(datetime.datetime.utcnow().timestamp())}")

            best_seller_rates.update(bsr) 
        
        chunks = divide_chunks(best_seller_rates, 100)
        

        if chunks:
            logger.debug(f"Start to insert {self.rank_limit * len(self.category_ids)} entries to a processing queue")

            # add entries from different categories to a queue with collection rotations
            for index, chunk in enumerate(chunks):
                try:
                    new_doc = frappe.get_doc(
                        {
                            "doctype": "Keepa Retrieving Queue Item Holder",
                            "name": f'{self.name}-{index}',
                            "analysis_link": self.name,
                            "item_list": json.dumps(chunk),
                            "priority":  PriorityEnum.LOW.value,
                            "item_type": 'ASIN'
                        }).insert()
                    frappe.db.commit()
                
                except frappe.exceptions.DuplicateEntryError as e:
                    logger.error(str(e))
                    break   
            frappe.db.commit()
            
            logger.debug(f"BSR entries successfully pushed to a processing queue")

