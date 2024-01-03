import logging
import psycopg2

import psycopg2.pool as psycopg2_pool

from migration.connector.destination.base import Destination
from migration.base.exceptions import DestinationExecutionError, GrammarRestrictionsError, SourceExecutionError

logger = logging.getLogger(__name__)

MAX_GET_STATUS_TIMES = 100


class PGDestination(Destination):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('PostgreSQL', config)
        self.connection = None
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_conf_path = storage_conf_path
        pool_config = {'host': self.connection_params['host'], 'port': self.connection_params['port'],
                       'user': self.connection_params['user'], 'password': self.connection_params['password']}
        self.pool = psycopg2_pool.SimpleConnectionPool(minconn=10, maxconn=30, **pool_config)

    def get_connection_params(self):
        assert self.config['host']
        assert self.config['user']
        assert self.config['password']
        assert self.config['port']

        return {
            'host': self.config['host'],
            'port': int(self.config['port']),
            'user': self.config['user'],
            'password': self.config['password']
        }

    def connect(self):
        if self.connection is None:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info(f"Connect to PG {self.connection_params['host']} successfully")

    def execute_sql(self, sql, bind_params=None):
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cur:
                cur.execute(sql, bind_params)
                result = cur.fetchall()
                return result

        except SourceExecutionError as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"
        except Exception as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"
        finally:
            self.pool.putconn(connection)

    def close(self):
        if self.connection is not None:
            self.connection.close()
        self.pool = None

    def quote_character(self):
        return "\""
