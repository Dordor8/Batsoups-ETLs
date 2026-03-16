import ast
import json
import os
import uuid

from faker import Faker

output_folder = os.getenv("OUTPUT_FOLDER")
fake = Faker()

ids = [331075960, 216596315]
phone_numbers = ["0546543224", "0534552345", "0527654321"]


def generate_json_message():
    return {
        "id": fake.random_element(elements=ids),
        "full_name": fake.name(),
        "address": fake.address().replace("\n", ", "),
        "phone": fake.random_element(elements=phone_numbers),
        "covid_test": fake.boolean(),
        "created_at": fake.date_time_this_year().isoformat(),
    }


def csv_to_dict(message):
    return dict(ast.literal_eval(message))


def parse_message(message):
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return csv_to_dict(message)
    except Exception:
        raise


def extract(max_messages=1):
    result = []
    for message in range(max_messages):
        raw = json.dumps(generate_json_message())
        try:
            data = parse_message(raw)
            print(data)
            result.append(data)
        except Exception:
            print("unexpected value, not a json")

    filename = f"file_{uuid.uuid4()}.json"
    output_path = os.path.join(output_folder, filename)
    os.makedirs(output_folder, exist_ok=True)
    with open(output_path, "w") as file:
        json.dump(result, file)


if __name__ == "__main__":
    extract()