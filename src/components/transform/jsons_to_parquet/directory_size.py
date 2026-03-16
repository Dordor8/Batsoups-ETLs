import json
import os


def get_dir_size_in_bytes(folder_path: str):
    total = 0
    try:
        for entry in os.scandir(r"C:\Users\User\Downloads\validated_jsons"):
            if entry.is_file():
                total += os.path.getsize(entry.path)
            elif entry.is_dir():
                total += get_dir_size_in_bytes(entry.path)
    except NotADirectoryError:
        return os.path.getsize(folder_path)

    return total
