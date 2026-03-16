import json
import logging
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, String, Integer, Float, Boolean, Date, DateTime, Text, MetaData, Column, Table, \
    inspect

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TYPE_MAPPING = {
    "integer": Integer,
    "string": String,
    "float": Float,
    "boolean": Boolean,
    "date": Date,
    "datetime": DateTime,
    "text": Text,
}


def file_format(file_name, path):
    if file_name.endswith(".json"):
        with open(path, "r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
        df = pd.json_normalize(data)
        logging.info(f"json file is loaded- {file_name}")
    else:
        raise Exception("the format must be json")
    return df


def define_engine():
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    database = os.getenv('DATABASE')
    engine = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}/{3}'.
        format(user, password, host, database))
    return engine


def create_table_from_schema(engine, table_name, schema):
    metadata = MetaData()
    columns = []
    for column_name, column_details in schema['properties'].items():
        column_type = TYPE_MAPPING.get(column_details.get("type"), String)
        is_pk = column_details.get('primary_key', False)
        new_column = Column(column_name, column_type, primary_key=is_pk)
        columns.append(new_column)
    Table(table_name, metadata, *columns)
    metadata.create_all(engine)
    logging.info(f"creating table {table_name} from the given schema")


def load_to_sql(folder, file_name, table_name, engine):
    file_path = os.path.join(folder, file_name)
    df = file_format(file_name, file_path)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logging.info(f"data from {file_name} inserted to {table_name}")


def main():
    load_dotenv(".env")
    folder = os.getenv("FOLDER")
    table_name = os.getenv("TABLE_NAME")
    schema_from_api = json.loads(os.getenv("SCHEMA"))

    engine = define_engine()
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        raise Exception("table already exists: ", table_name)

    create_table_from_schema(engine, table_name, schema_from_api)
    logging.info(f"{table_name} created successfully")

    for file_name in os.listdir(folder):
        if file_name.endswith(".json"):
            load_to_sql(folder, file_name, table_name, engine)


if __name__ == "__main__":
    main()
