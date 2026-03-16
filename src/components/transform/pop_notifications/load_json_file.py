import json
import logging
import os

import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json_file(path, file_name):
    with open(path, "r", encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)
    df = pd.json_normalize(data)
    logging.info(f"json file is loaded- {file_name}")
    return df


def notify_positive_covid_test(df,)

def extract_bdt(schema):
    bdt = schema.get("bdt", {})
    phone = None
    covid_test = None
    for col, meaning in bdt.items():
        if meaning == "PHONE":
            phone= col
        if meaning == "COVID_TEST":
            covid_test= col
    return phone, covid_test


def main():
    load_dotenv(".env")
    folder = os.getenv("FOLDER")
    schema = json.loads(os.getenv('SCHEMA'))
    extract_bdt(schema)




