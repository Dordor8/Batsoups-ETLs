import json
import logging

import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json_file(path, file_name):
    with open(path, "r", encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)
    df = pd.json_normalize(data)
    logging.info(f"json file is loaded- {file_name}")
    return df


