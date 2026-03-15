import json
import os.path

import pandas as pd


def file_format(file_name, path):
    if file_name.endswith(".csv"):
        df = pd.read_csv(path)
    elif file_name.endswith('.json'):
        with open(path, "r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
        df = pd.json_normalize(data)
    else:
        raise Exception("the formats are csv and json")
    return df


def load_to_sql(folder, file_name, table_name, schema_from_api):
    file_path = os.path.join(folder, file_name)
    df = file_format(file_name, file_path)


def main():
    ...
