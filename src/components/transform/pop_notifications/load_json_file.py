import json
import logging
import os

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json_file(path, file_name):
    with open(path, "r", encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)
    df = pd.json_normalize(data)
    logging.info(f"json file is loaded- {file_name}")
    return df


def send_notifications(phone, string_message):
    url = os.getenv("API_URL")
    payload = {
        "phone": phone,
        "message": string_message
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        logging.info(f" notification sent to {phone}")
    else:
        logging.warning(f"failed sending to {phone}")


def notify_positive_covid_test(df, phone_as_string, covid_test_as_string):
    if not phone_as_string or not covid_test_as_string:
        logging.warning("phone or covid test fields ar missing in the BDT")
        return
    for _, row in df.iterrows():
        phone = row.get(phone_as_string)
        test = row.get(covid_test_as_string)

        if test:
            send_notifications(phone, "you are positive for Covid19! you must go into isolation immediately!!!!")


def extract_bdt(schema):
    bdt = schema.get("bdt", {})
    phone = None
    covid_test = None
    for col, meaning in bdt.items():
        if meaning == "PHONE":
            phone = col
        if meaning == "COVID_TEST":
            covid_test = col
    return phone, covid_test


def transform_json_files_in_folder(folder, schema):
    phone, covid_test = extract_bdt(schema)
    logging.info(f"phone and covid test result: {phone, covid_test}")

    for file_name in os.listdir(folder):
        if file_name.endswith(".json"):
            path = os.path.join(folder, file_name)
            df = load_json_file(path, file_name)
            notify_positive_covid_test(df, phone, covid_test)



