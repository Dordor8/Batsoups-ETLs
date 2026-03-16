import ast
import json
import os
import uuid

from dotenv import load_dotenv
from validate_full_name import validate_full_name
from validate_id import validate_israeli_id
from validate_phone_number import validate_phone_number
from validate_address import validate_address
from validate_covid_result import validate_covid_test_result
from s3_loader.s3_handler.write_to_s3 import write_to_s3

load_dotenv()
json_folder_path = os.getenv("folder_path")
schema = os.getenv("schema")
schema_as_dict = ast.literal_eval(schema)
bdt = schema_as_dict["bdt"]
s3_prefix = os.getenv("s3_prefix")
validated_jsons_folder_path = os.getenv("validated_jsons_folder_path")


def validate_bdt():
    for filename in os.listdir(json_folder_path):
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
                            validate_israeli_id(json_bdt_value)
                        case "full_name":
                            validate_full_name(json_bdt_value)
                        case "phone_number":
                            validate_phone_number(json_bdt_value)
                        case "address":
                            validate_address(json_bdt_value)
                        case "covid_test_result":
                            validate_covid_test_result(json_bdt_value)
                except ValueError:
                    error = True
            if error:
                write_to_s3(single_json, s3_prefix)
            else:
                validated_path = os.path.join(validated_jsons_folder_path, f"validated_{uuid.uuid4()}.json")
                with open(validated_path, "w") as f:
                    json.dump(single_json, f)
