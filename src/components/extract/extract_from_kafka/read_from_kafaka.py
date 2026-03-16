import ast
import json
import os
import uuid

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()
output_folder = os.getenv("OUTPUT_FOLDER")
topic = os.getenv("TOPIC")
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
group_id = os.getenv("GROUP_ID")
inactive_time_ms = os.getenv("INACTIVE_TIME")
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers.split(','),
                         group_id=group_id,
                         consumer_timeout_ms=inactive_time_ms)


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
    count = 0
    for message in consumer:
        # print(message)
        try:
            data = parse_message(message.value.decode("utf-8"))
            result.append(data)
            count += 1
        except Exception:
            print("unexpected value, not a csv or json")
        finally:
            consumer.commit()
        if count >= max_messages:
            break

    filename = f"file_{uuid.uuid4()}.json"
    output_path = os.path.join(output_folder, filename)
    with open(output_path, "w") as file:
        json.dump(result, file)


if __name__ == "__main__":
    extract()
