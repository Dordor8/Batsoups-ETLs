def validate_covid_test_result(result):
    return type(result) is bool


if __name__ == "__main__":
    print(validate_covid_test_result(True))
