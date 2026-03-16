def validate_full_name(name):
    if not type(name) is str:
        return False
    split_name = name.split()
    return len(split_name) > 1


if __name__ == "__main__":
    print(validate_full_name("talia dfsd fdgd"))
