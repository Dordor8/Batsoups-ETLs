import os


def get_dir_size_in_bytes(folder_path: str):
    total = 0
    try:
        for entry in os.scandir(folder_path):
            if entry.is_file():
                total += os.path.getsize(entry.path)
            elif entry.is_dir():
                total += get_dir_size_in_bytes(entry.path)
    except NotADirectoryError:
        return os.path.getsize(folder_path)

    return total


if __name__ == "__main__":
    print(get_dir_size_in_bytes(r"C:\Users\User\Documents\json_test"))
