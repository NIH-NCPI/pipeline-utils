from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.general import *


def format_tests(tests, enums=None):
    """
    Formats the tests string into dbt-compatible test definitions.

    Args:
        tests: A pipe-delimited string of tests, e.g., "not_null|accepted_values".
        enums: enumeration values, e.g., 'HTP', 'Other'.
                                Used when "accepted_values" is one of the tests.

    Returns:
        list: A list of dictionaries representing the formatted dbt tests.
    """
    test_list = tests.split("|")

    if enums:
        enums = [enum.strip() for enum in enums.split(";")]

    formatted_tests = []

    for test in test_list:
        test = test.strip() 

        if test == "accepted_values" and enums:  
            formatted_tests.append({
                "accepted_values": {
                    "values": enums
                }
            })
        # if enums:
        #     formatted_tests.append(
        #         {"not_accepted_partial_match": {"values": ["FTD_FLAG"]}}
        #     )
        if test == "not_null":
            formatted_tests.append(test)

    return formatted_tests
