import logging

from odps import ODPS
import odps.errors

from migration.connector.destination.base import Destination
from migration.base.exceptions import DestinationExecutionError, GrammarRestrictionsError

logger = logging.getLogger(__name__)

MAX_GET_STATUS_TIMES = 100


class OdpsDestination(Destination):
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

    def create_table(self, table_name, ddl: str):
        try:
            self.odps.execute_sql(ddl)
            logger.info(f"Odps Create table {table_name} successfully")
        except odps.errors.ODPSError as e:
            logger.error(f"Odps Create table {table_name} failed, error: {e}")
            raise f"Odps Create table {table_name} failed, error: {e}"

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
        except DestinationExecutionError as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"
        except Exception as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"

    def close(self):
        logger.info(f"Odps connector close successfully")