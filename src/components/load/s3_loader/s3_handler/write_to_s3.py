import os
import boto3
from dotenv import load_dotenv

load_dotenv()
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
group_name = os.getenv("group_name")
bucket_name = os.getenv("bucket_name")


def write_to_s3(source_path: str, prefix: str):
    print(group_name)

    print(f"Writing to key: {bucket_name + group_name + prefix}")

    try:
        s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        if os.path.isfile(source_path):
            file_name = os.path.basename(source_path)
            with open(source_path, 'rb') as data:
                s3.Bucket(bucket_name).put_object(Key=group_name + prefix + file_name, Body=data)
            print(f'File {source_path} uploaded to S3 bucket {bucket_name}')

        elif os.path.isdir(source_path):
            for root, _, files in os.walk(source_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, source_path)
                    with open(file_path, 'rb') as file_data:
                        s3.upload_file(file_data, bucket_name, relative_path)
                    print(f'File {file_path} uploaded to S3 bucket {bucket_name}')
        else:
            print(f'Invalid source path: {source_path}')
            return False

        return True
    except Exception as e:
        print(f'Error uploading files to S3 bucket {bucket_name}: {e}')
        return False


if __name__ == "__main__":

    write_to_s3(r"C:\Users\User\Downloads\epidemiology.csv", "json-files/")
