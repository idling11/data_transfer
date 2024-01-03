import logging
from migration.connector.source.enum import Column

from migration.base.status import Status

logger = logging.getLogger(__name__)


class Destination:
    def __init__(self, name, config: dict):
        self.name = name
        self.config = config
        self.connection = None

    def get_connection_params(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"

    def execute_sql(self, sql, bind_params=None):
        raise NotImplementedError

    def close(self):
        pass

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        raise NotImplementedError

    def load_external_data(self, file_path, schema_name, table_name):
        raise NotImplementedError

    def gen_destination_ddl(self, db_name, table_name, columns, primary_keys, cluster_info, partition_columns):
        raise NotImplementedError

    def migration_test_connection(self):
        try:
            self.connect()
            self.close()
            return True
        except Exception as e:
            logger.error("Test connecting to destination Error :{}", e)
            return False

    def create_database(self, db_name):
        raise NotImplementedError

    def create_table(self, table_name, ddl: str):
        raise NotImplementedError

    def quote_character(self):
        return "`"
