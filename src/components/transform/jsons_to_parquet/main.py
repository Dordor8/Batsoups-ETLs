import glob
import uuid
import pyarrow as pa
import pyarrow.json as pa_json
import pyarrow.parquet as pa_parquet
import os


config = {
  "minimal_size": 0.256,
  "maximal_size": 1
}

max_size = config["maximal_size"]
min_size = config["minimal_size"]

output_path = os.getenv("OUTPUT_PATH")
input_path = os.getenv("INPUT_FOLDER")


def get_dir_size_in_bytes(folder_path: str):
    total = 0
    try:
        for entry in os.scandir(input_path):
            if entry.is_file():
                total += os.path.getsize(entry.path)
            elif entry.is_dir():
                total += get_dir_size_in_bytes(entry.path)
    except NotADirectoryError:
        return os.path.getsize(folder_path)

    return total


def convert_folder_to_parquet():
    _dirct_folder_conversion()

def _convert_jsons_to_parquet():
    json_files = glob.glob(os.path.join(input_path, "*.json"))
    tables = []
    for json_file in json_files:
        tables.append(pa_json.read_json(json_file))
    merged_tables = pa.concat_tables(tables)
    parquet_file = f"{uuid.uuid4()}.parquet"
    full_path = os.path.join(output_path, parquet_file)
    os.makedirs(output_path, exist_ok=True)
    with open(full_path, 'wb') as f:
        pa_parquet.write_table(merged_tables, f)


def _dirct_folder_conversion():
    folder_size = get_dir_size_in_bytes(input_path) / (1024 ** 3)
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
