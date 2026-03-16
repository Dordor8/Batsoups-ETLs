import stdnum.il.idnr


def validate_israeli_id(person_id: str) -> bool:
    return stdnum.il.idnr.is_valid(person_id)


if __name__ == "__main__":
    print(validate_israeli_id("331075960"))