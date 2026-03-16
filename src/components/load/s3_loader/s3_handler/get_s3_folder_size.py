import os

import boto3
from dotenv import load_dotenv

load_dotenv()
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
group_name = os.getenv("group_name")
bucket_name = os.getenv("bucket_name")


def get_s3_folder_size(prefix: str):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        return 0

    total = sum(obj['Size'] for obj in response['Contents'])
    return total / (1024 ** 3)
