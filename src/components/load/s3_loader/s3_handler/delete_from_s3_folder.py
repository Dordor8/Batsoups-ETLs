import os
import boto3
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
group_name = os.getenv("group_name")
bucket_name = os.getenv("bucket_name")


def delete_from_s3_folder(prefix: str):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        print(f"No files found in {prefix}")
        return False

    objects = [{'Key': obj['Key']} for obj in response['Contents']]
    s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
    print(f"Deleted {len(objects)} files from {prefix}")
    return True


if __name__ == "__main__":
    delete_from_s3_folder("group2/json-files/")
