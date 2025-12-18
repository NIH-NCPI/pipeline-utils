class DatabaseBC():

    def __init__(self, pipeline_db):
        self.pipeline_db = pipeline_db

    @classmethod
    def connect_db(cls, pipeline_db,):
        if pipeline_db == 'duckdb':
            return DuckdbClass()
        elif pipeline_db == 'pg':
            return PostgresqlClass()

    def generate_src_sql_files(self, output_dir):
        pass


class DuckdbClass(DatabaseBC):
    def __init__(self, pipeline_db):
        super().__init__(pipeline_db)
        self.new_att = "brenda_test ddb"

    def import_data(self):
        pass


class PostgresqlClass(DatabaseBC):
    def __init__(self, pipeline_db):
        super().__init__(pipeline_db)
        self.new_att2 = "brenda_test_pg"

    def import_data(self):
        pass

