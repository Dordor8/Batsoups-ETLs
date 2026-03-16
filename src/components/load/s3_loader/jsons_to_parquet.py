import json
from s3_loader.check_folder_size import get_dir_size_in_bytes
from s3_loader.convert_jsons_to_paruet import convert_to_parquet
import os
from dotenv import load_dotenv
import threading
from s3_loader.s3_handler.delete_from_s3_folder import delete_from_s3_folder
from s3_loader.s3_handler.write_to_s3 import write_to_s3
from s3_loader.s3_handler.get_s3_folder_size import get_s3_folder_size

load_dotenv()
group_name = os.getenv("group_name")
bucket_name = os.getenv("bucket_name")
folder_path = os.getenv("folder_path")

with open('data.json', 'r') as file:
    config = json.load(file)

max_size = config["maximal_size"]
min_size = config["minimal_size"]
json_prefix = config["json_prefix"]
output_path = bucket_name + group_name + "/parquet-files/output.parquet"

current_folder_size = 0


def check_if_size_matches(folder_size):
    return max_size > folder_size > min_size


# TODO: add schema handling
def convert_jsons_to_parquet():
    global current_folder_size
    folder_size = get_dir_size_in_bytes(folder_path) / (1024 ** 3)
    if check_if_size_matches(folder_size):
        convert_to_parquet()
    elif folder_size < min_size:
        write_to_s3(bucket_name, folder_path)
        folder_size = get_s3_folder_size(bucket_name)
        if check_if_size_matches(folder_size):
            convert_to_parquet()
            delete_from_s3_folder(group_name + json_prefix)
        else:
            reset_timer()
    else:
        convert_to_parquet() # temp
        # folder is too big - needs to be split


inactivity_timer = None


def no_new_data():
    convert_to_parquet()
    delete_from_s3_folder(group_name + json_prefix)


def reset_timer():
    global inactivity_timer
    inactivity_timer = threading.Timer(config["inactivity_time"], no_new_data)
    inactivity_timer.start()
