# Move these to env vars or a common file
SRC_SCHEMA = "dbo_src_data"
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
