import glob
import os.path
import pyarrow as pa
import pyarrow.json as pa_json
import pyarrow.parquet as pa_parquet
import os
import s3fs
from transfrom.merge_jsons import merge_jsons

aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
group_name = os.getenv("group_name")
bucket_name = os.getenv("bucket_name")


def convert_to_parquet(folder_path: str, output_path: str):
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    tables = []
    for file in json_files:
        tables.append(pa_json.read_json(file))
    merged_tables = pa.concat_tables(json_files)
    fs = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key
    )
    with fs.open(output_path, 'wb') as f:
        pa_parquet.write_table(merged_tables, f)


if __name__ == "__main__":
    convert_to_parquet(r"C:\Users\User\Documents\json_test",
                       r"devops-training-504956989193/group2/parquet-files/output.parquet")
