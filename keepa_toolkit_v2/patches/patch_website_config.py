import os
import json

from loguru import logger


def execute():
    with open('common_site_config.json', 'r') as file:
        config = json.loads(file.read())
    
    if not config.get('workers'):
        config['workers'] = {}
    
    if not config['workers'].get('keepa_queue'):
        config['workers']['keepa_queue'] = {
            "timeout": 5000, 
            "background_workers": 4
        }
        
    with open('common_site_config.json', 'w') as file:
        raw_json = json.dumps(config)
        file.write(raw_json)
        logger.info('Queue keepa_queue registred in common_sites_config.json')