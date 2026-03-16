import os
from s3_handler.write_to_s3 import write_to_s3

from dotenv import load_dotenv

load_dotenv()
folder_path = os.getenv("folder_path")
parquet_s3_prefix = os.getenv("parquet_s3_prefix")


def write_parquet_to_s3():
    write_to_s3(folder_path, parquet_s3_prefix)


