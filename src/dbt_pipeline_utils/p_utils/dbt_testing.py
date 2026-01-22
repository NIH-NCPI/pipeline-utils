
    
class DbtTesting:

    def format_tests(tests, col, enums=None):
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

        if enums is not None:
            enums = [enum.strip() for enum in enums.split(";")]

        formatted_tests = []
        is_required = "not_null" in test_list

        for test in test_list:
            test = test.strip()

            if test == "accepted_values" and enums and not is_required:
                formatted_tests.append(
                    {
                        "accepted_values": {
                            "values": enums,
                            "config": {"where": f"{col} is not null"},
                        }
                    }
                )
            if test == "accepted_values" and enums and is_required:
                formatted_tests.append({"accepted_values": {"values": enums}})

            if test == "not_null":
                formatted_tests.append(test)

        return formatted_tests
