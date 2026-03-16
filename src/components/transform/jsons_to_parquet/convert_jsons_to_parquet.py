import glob
import json
import os.path
import uuid
import pyarrow as pa
import pyarrow.json as pa_json
import pyarrow.parquet as pa_parquet
import os
from dotenv import load_dotenv
from directory_size import get_dir_size_in_bytes

with open('optimal_size.json', 'r') as file:
    config = json.load(file)

max_size = config["maximal_size"]
min_size = config["minimal_size"]

load_dotenv()
output_path = os.getenv("output_path")
folder_path = os.getenv("folder_path")


def convert_folder_to_parquet():
    _dirct_folder_conversion()

def _convert_jsons_to_parquet():
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    tables = []
    for json_file in json_files:
        tables.append(pa_json.read_json(json_file))
    merged_tables = pa.concat_tables(tables)
    parquet_file = f"{uuid.uuid4()}.parquet"
    full_path = os.path.join(output_path, parquet_file)
    with open(full_path, 'wb') as f:
        pa_parquet.write_table(merged_tables, f)


def _dirct_folder_conversion():
    folder_size = get_dir_size_in_bytes(folder_path) / (1024 ** 3)
    if max_size > folder_size > min_size:
        _convert_jsons_to_parquet()
    elif folder_size < min_size:
        _convert_jsons_to_parquet()
        # throw into s3 and set a timer
        pass
    else:
        _convert_jsons_to_parquet()
        # split folder into smaller ones and then convert to parquet
        pass


if __name__ == "__main__":
    convert_folder_to_parquet()
