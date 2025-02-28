 # Move these to env vars or a common file
RAW_SCHEMA = "dbo_raw_data"
DB_HOST = "localhost"
DB_USER = "gutmanb"
DB_NAME = "default_db"


# Map common data types to PostgreSQL types
type_mapping = {
    "string": "TEXT",
    "integer": "INTEGER",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "datetime": "TIMESTAMP"
}

m00m00_ftd_tables = ['participant','condition','event','measurement']