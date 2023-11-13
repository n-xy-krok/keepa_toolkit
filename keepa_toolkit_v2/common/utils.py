
from decimal import Decimal
import json
import re
from typing import Generator, Any, Iterable

import pandas as pd
import numpy as np
from requests import post


def convert_to_basic_types(product: dict):
    for key, value in product['data'].items():

        if isinstance(product['data'][key], pd.DataFrame):
            product['data'][key] = json.loads(value.to_json())

        if isinstance(product['data'][key], np.ndarray):
            product['data'][key] = value.tolist()


def find_pack_number(text):
    if not text:
        return 1
    pattern = r'Pack of (\d+)'
    match = re.search(pattern, text)
    if match:
        return int(match.group(1))
    else:
        return 1


def calculate_prep_cost(count_on_amazon: int):
    costs = {
        2: Decimal('1.08'),
        3: Decimal('1.26'),
        4: Decimal('1.44'),
        5: Decimal('1.71'),
        6: Decimal('1.94'),
    }
    if count_on_amazon in range(2, 6 + 1):
        return costs[count_on_amazon]

    if count_on_amazon > 6:
        return Decimal('2.25')

    return Decimal('0.78')


def escape_string(string: str) -> str:
    if not string:
        return ''
    string = string.replace("'", "`")
    return string

def send_tg_report(text: str):
    try:
        post(f'https://api.telegram.org/bot1473438682:AAEX_mJfU6XrGFhxegpkfXut_ysURCZZkHc/sendMessage?chat_id=621700759&text={text}')
    except:
        ...
        
def divide_chunks(collection: Iterable, n) -> Generator[list[str], Any, Any]:
    collection = list(collection)
    for i in range(0, len(collection), n): 
        yield collection[i:i + n]

