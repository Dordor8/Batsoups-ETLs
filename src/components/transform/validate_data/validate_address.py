from mikud import Mikud


def validate_address(address):
    return type(address) is str


if __name__ == "__main__":
    validate_address("תל-אביב יד המעביר 1")
