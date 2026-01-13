# Map common data types to PostgreSQL types
type_mapping = {
    "string": "text",
    "integer": "integer",
    "float": "float",
    "boolean": "boolean",
    "datetime": "timestamp"
}

import re

SAFE_CHARS = re.compile(r"[^a-zA-Z0-9_]+")
MAX_IDENTIFIER_LEN = 60  

# Define mappings for different dictionary formats.
DD_FORMATS = {
    "src_type1": {
        "variable_name": "Variable Name",
        "formatted_variable_name": "Variable Name",  # Utils generated, for stg dds. Not from data source.
        "description": "Description",
        "data_type": None,
        "min": None,
        "max": None,
        "units": None,
        "enumerations": None,
        "comment": None,
        "src_variable_name": "src_variable_name",  # Utils generated, for stg dds. Not from data source.
    },
    # Generated/intermediate pipeline dds, will follow the same format (pipeline_format)
    "ftd_dd": {
        "variable_name": "variable_name",
        "formatted_variable_name": "variable_name", 
        "description": "description",
        "data_type": "data_type",
        "min": "min",
        "max": "max",
        "units": "units",
        "enumerations": "enumerations",
        "comment": "comment",
        "src_variable_name": "src_variable_name",
        "tests": "tests",
    },
    # Generated/intermediate pipeline dds, will follow the same format (pipeline_format)
    "pipeline_format": {
        "variable_name": "variable_name",
        "formatted_variable_name": "variable_name",
        "description": "variable_description",
        "data_type": "data_type",
        "min": "min",
        "max": "max",
        "units": "units",
        "enumerations": "enumerations",
        "comment": "comment",
        "src_variable_name": "src_variable_name",
        "tests": "tests",
    },
}
