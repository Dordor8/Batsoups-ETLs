import json
import os

from dotenv import load_dotenv

from src.components.transform.pop_notifications.load_json_file import transform_json_files_in_folder


def main():
    load_dotenv(".env")
    folder = os.getenv("FOLDER")
    schema = json.loads(os.getenv('SCHEMA'))
    transform_json_files_in_folder(folder, schema)


if __name__ == '__main__':
    main()

