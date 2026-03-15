import json
import logging
import os.path

import pandas as pd
from dotenv import load_dotenv
from jsonschema2ddl import JSONSchemaToDatabase
from sqlalchemy import create_engine, inspect


def file_format(file_name, path):
    if file_name.endswith(".csv"):
        df = pd.read_csv(path)
        logging.info(f"csv file is loaded- {file_name}")
    elif file_name.endswith('.json'):
        with open(path, "r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
        df = pd.json_normalize(data)
        logging.info(f"json file is loaded- {file_name}")
    else:
        raise Exception("the formats are csv and json")
    return df


def define_engine(table_name):
    load_dotenv("./.env")
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    database = table_name
    engine = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}/{3}'.
        format(user, password, host, database))
    return engine


def load_to_sql(folder, file_name, table_name, schema_from_api):
    file_path = os.path.join(folder, file_name)
    df = file_format(file_name, file_path)

    engine = define_engine(table_name)
    conn = engine.raw_connection()
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        raise Exception("table already exists: ", table_name)

    logging.info(f"creating table {table_name} from the given schema")
    translator = JSONSchemaToDatabase(
        schema_from_api,
        root_table_name=table_name,
    )

    translator.create_tables(conn)
    translator.create_links(conn)
    translator.analyze(conn)
    conn.commit()
    conn.close()
    logging.info(f"{table_name} created successfully")

    df.to_sql(table_name, engine, if_exists='append', index=False)
    logging.info(f"data inserted to {table_name}")


def main():
    folder = os.getenv("FOLDER")
    file_name = os.getenv("FILE_NAME")
    table_name = os.getenv("TABLE_NAME")
    schema = json.loads(os.getenv("SCHEMA"))

    load_to_sql(folder, file_name, table_name, schema)


if __name__ == "__main__":
    main()
