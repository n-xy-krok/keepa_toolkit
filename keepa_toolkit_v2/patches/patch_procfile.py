import os

from loguru import logger


def execute():
    with open('../Procfile', 'a+') as file:
        file.writelines(["worker_keepa_queue: bench worker --queue keepa_queue 1>> logs/worker_keepa_queue.log 2>> logs/worker_keepa_queue.error.log"])
    
    logger.info('keepa_queue worker created in Procfile')