import logging
import odps.errors
from odps import ODPS

from migration.connector.source.enum import Column

from migration.connector.source.base import Source
from migration.base.exceptions import SourceExecutionError

logger = logging.getLogger(__name__)


class OdpsSource(Source):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Odps', config)
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_config_path = storage_conf_path
        self.odps = ODPS(self.connection_params['id'], self.connection_params['key'],
                         self.connection_params['project'], self.connection_params['endpoint'])
        

    def get_connection_params(self):
        assert self.config['id']
        assert self.config['key']
        assert self.config['project']
        assert self.config['endpoint']

        return {
            'id': self.config['id'],
            'key': self.config['key'],
            'project': self.config['project'],
            'endpoint': self.config['endpoint'],
        }

    def connect(self):
        try:
            self.odps.execute_sql("select 1")
            logger.info(f"Connect to Odps {self.connection_params['endpoint']} successfully")
        except odps.errors.ODPSError as e:
            logger.error(f"Connect to Odps {self.connection_params['endpoint']} failed, error: {e}")
            raise f"Connect to Odps {self.connection_params['endpoint']} failed, error: {e}"

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        result = self.odps.get_table(table_name)
        table_columns = []
        for row in result.columns:
            table_columns.append(Column(name=row.name, type=row.type.name.upper(),
                                        is_null=row.type.nullable,
                                        default_value=None))
        return table_columns

    def execute_sql(self, sql, bind_params=None):
        try:
            result = []
            with self.odps.execute_sql(sql).open_reader() as reader:
                for record in reader:
                    result.append(record.values)
            return result
        except odps.errors.ODPSError as e:
            logger.error(f"Odps connector execute sql {sql} failed, error: {e}")
            raise f"Odps connector execute sql {sql} failed, error: {e}"
        except SourceExecutionError as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"
        except Exception as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"

    def type_mapping(self):
        return {}

    def close(self):
        logger.info(f"Close connection to odps {self.connection_params['endpoint']} successfully")

    def int_type_string(self):
        return 'INT'

    def quote_character(self):
        return "`"
