import json
import uuid
import stdnum.il.idnr
from phonenumbers import is_valid_number, region_code_for_number, parse
import os
import boto3


json_folder_path = os.getenv("INPUT_FOLDER")
schema = os.getenv("SCHEMA")
workflow_name = os.getenv("WORKFLOW_NAME")
validated_jsons_folder_path = os.getenv("OUTPUT_FOLDER")

aws_access_key_id = os.getenv("ACCESS_KEY")
aws_secret_access_key = os.getenv("SECRET_ACCESS")
group_name = os.getenv("GROUP_NAME")
bucket_name = os.getenv("BUCKET_NAME")

schema_as_dict = json.loads(schema)
bdt = schema_as_dict["BDT"]

region = "IL"

INVALID_FOLDER = "failed-validation-data/" + workflow_name + "/"


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

def validate_phone_number(phone_number):

    if not isinstance(phone_number, (str, int)):
        return False
    phone_number = str(phone_number)
    parsed_number = parse(phone_number, region)
    return is_valid_number(parsed_number) and region_code_for_number(parsed_number) == region


def validate_israeli_id(person_id: str) -> bool:
    return stdnum.il.idnr.is_valid(person_id)


def validate_full_name(name):
    if not type(name) is str:
        return False
    split_name = name.split()
    return len(split_name) > 1


def validate_address(address):
    return type(address) is str


def validate_covid_test_result(result):
    return type(result) is bool


def validate_bdt():
    for filename in os.listdir(json_folder_path):
        result = True
        with open(os.path.join(json_folder_path, filename)) as f:
            json_data = json.load(f)
        for single_json in json_data:
            error = False
            for bdt_value, matching_json in bdt.items():
                try:
                    json_bdt_value = single_json[matching_json]
                except Exception:
                    error = True
                    continue
                try:
                    match bdt_value:
                        case "id":
                            result = validate_israeli_id(json_bdt_value)
                        case "full_name":
                            result = validate_full_name(json_bdt_value)
                        case "phone_number":
                            result = validate_phone_number(json_bdt_value)
                        case "address":
                            result = validate_address(json_bdt_value)
                        case "covid_test_result":
                            result = validate_covid_test_result(json_bdt_value)
                    if result is False:
                        raise ValueError(f"Validation failed for {bdt_value}")
                except ValueError:
                    error = True
            if error:
                print("unvalidated")
                write_to_s3(single_json, INVALID_FOLDER)
            else:
                print("validated")
                validated_path = os.path.join(validated_jsons_folder_path, f"validated_{uuid.uuid4()}.json")
                with open(validated_path, "w") as f:
                    json.dump(single_json, f)


if __name__ == "__main__":
    validate_bdt()
