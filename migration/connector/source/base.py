import logging
import os
from datetime import datetime
from migration.connector.source.enum import Column
import json

logger = logging.getLogger(__name__)


class Source:
    def __init__(self, name, config: dict):
        self.name = name
        self.config = config
        self.source_id = self._gen_source_id()
        self.connection = None
        self.connection_params = None

    def get_connection_params(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def get_database_names(self):
        raise NotImplementedError

    def get_table_names(self, database_name):
        raise NotImplementedError

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        raise NotImplementedError

    def get_ddl_sql(self, database_name, table_name):
        raise NotImplementedError

    def get_table_cluster_info(self, database_name, table_name):
        raise NotImplementedError

    def get_table_partition_columns(self, database_name, table_name):
        raise NotImplementedError

    def get_primary_key(self, database_name, table_name):
        raise NotImplementedError

    def execute_sql(self, sql, bind_params=None):
        raise NotImplementedError

    def type_mapping(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def _gen_source_id(self):
        return f"{self.name}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"

    def unload_data(self, task):
        raise NotImplementedError

    def int_type_string(self):
        raise NotImplementedError

    def migration_test_connection(self):
        try:
            self.connect()
            self.close()
            return True
        except Exception as e:
            logger.error(f"Test connection to source failed: {e}")
            return False

    def quote_character(self):
        return "`"

    def get_table_pk_columns(self, database_name, table_name):
        raise NotImplementedError

