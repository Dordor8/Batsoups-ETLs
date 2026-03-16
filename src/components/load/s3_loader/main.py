import os
import boto3

aws_access_key_id = os.getenv("ACCESS_KEY")
aws_secret_access_key = os.getenv("SECRET_ACCESS")
group_name = os.getenv("GROUP_NAME")
bucket_name = os.getenv("BUCKET_NAME")
folder_path = os.getenv("INPUT_FOLDER")
parquet_s3_prefix = os.getenv("S3_PREFIX")

def write_to_s3(source_path: str, prefix: str):
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
                    s3.Bucket(bucket_name).upload_file(
                        file_path,
                        group_name + prefix + relative_path
                    )
                    print(f'File {file_path} uploaded to S3 bucket {bucket_name}')
        else:
            print(f'Invalid source path: {source_path}')
            return False

        return True
    except Exception as e:
        print(f'Error uploading files to S3 bucket {bucket_name}: {e}')
        return False

def write_parquet_to_s3():
    write_to_s3(folder_path, parquet_s3_prefix)


if __name__ == "__main__":
    write_parquet_to_s3()