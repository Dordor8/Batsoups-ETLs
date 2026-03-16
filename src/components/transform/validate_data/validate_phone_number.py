import phonenumbers
from phonenumbers import is_valid_number, region_code_for_number

region = "IL"


def validate_phone_number(phone_number):

    if not isinstance(phone_number, (str, int)):
        return False
    phone_number = str(phone_number)
    parsed_number = phonenumbers.parse(phone_number, region)
    return is_valid_number(parsed_number) and region_code_for_number(parsed_number) == region


if __name__ == "__main__":
    print(validate_phone_number("0531114447"))
