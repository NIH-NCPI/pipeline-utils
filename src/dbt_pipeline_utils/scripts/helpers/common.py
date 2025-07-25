# Map common data types to PostgreSQL types
type_mapping = {
    "string": "text",
    "integer": "integer",
    "float": "float",
    "boolean": "boolean",
    "datetime": "timestamp"
}

# Define mappings for different dictionary formats.
DD_FORMATS = {
    "src_type1": {
        "variable_name": "Variable Name",
        "formatted_variable_name": "Variable Name",
        "description": "Description",
        "data_type": None, 
        "min": None,
        "max": None,
        "units": None,
        "enumerations": None,
        "comment": None,
        "src_variable_name": "src_variable_name",  # Generated, for stg dds
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
        "tests": "tests"
    },
}