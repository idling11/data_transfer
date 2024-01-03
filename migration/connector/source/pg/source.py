import logging
import re
import time
from datetime import datetime
import psycopg2
from psycopg2 import pool as psycopg2_pool

from migration.connector.source.enum import Column

from migration.connector.source.base import Source
from migration.base.exceptions import SourceExecutionError

logger = logging.getLogger(__name__)


class PGSource(Source):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('PostgreSQL', config)
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_config_path = storage_conf_path
        pool_config = {'host': self.connection_params['host'], 'port': self.connection_params['port'],
                       'user': self.connection_params['user'], 'password': self.connection_params['password'],
                       'database': self.connection_params['database']
                       }
        self.pool = psycopg2_pool.SimpleConnectionPool(minconn=10, maxconn=30, **pool_config)

    """
    PostgreSQL connection parameters is a dict including following keys:
    1. host: host of PG
    2. port: port for PG client
    3. database: database name
    4. user: user name
    5. passwd: password
    """

    def get_connection_params(self):
        assert self.config['host']
        assert self.config['user']
        assert self.config['password']
        assert self.config['port']

        return {
            'host': self.config['host'],
            'port': int(self.config['port']),
            'user': self.config['user'],
            'password': self.config['password'],
            'database': self.config['database'],
        }

    def connect(self):
        if self.connection is None:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info(f"Connect to PG {self.connection_params['host']} successfully")

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        result = self.execute_sql(f"select column_name, data_type, is_nullable,column_default "
                                  f"from INFORMATION_SCHEMA.COLUMNS where table_name =  '{table_name}' and table_schema = '{database_name}'")
        table_columns = []
        for row in result:
            table_columns.append(Column(name=row[0], type=row[1].upper(),
                                        is_null=True if row[2].strip() == 'YES' else False,
                                        default_value=row[3]))
        return table_columns

    def execute_sql(self, sql, bind_params=None):
        connection = self.pool.getconn()
        results = []
        try:
            with connection.cursor() as cur:
                cur.execute(sql)
                if sql.strip().upper().startswith('CREATE') or sql.strip().upper().startswith('DROP'):
                    return []
                for row in cur:
                    results.append(row)
                return results

        except SourceExecutionError as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"

        except BaseException as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"

        except Exception as e:
            logger.error(f"PG connector execute sql {sql} failed, error: {e}")
            raise f"PG connector execute sql {sql} failed, error: {e}"
        finally:
            self.pool.putconn(connection)

    def type_mapping(self):
        return {
            'TIME': 'DATE',
            'CHARACTER': 'VARCHAR',
            'CHARACTER VARIATION': 'VARCHAR',
            'REAL': 'FLOAT',
            'DOUBLE PRECISION': 'DOUBLE',
            'INTEGER': 'INT',
            'TIMESTAMP WITHOUT TIME ZONE': 'TIMESTAMP',
        }

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.pool is not None:
            self.pool = None
        logger.info(f"Close connection to PG {self.connection_params['host']} successfully")

    def int_type_string(self):
        return 'INT'

    def quote_character(self):
        return "\""

    def get_table_pk_columns(self, database_name, table_name):
        result = self.execute_sql(f"select column_name from INFORMATION_SCHEMA.COLUMNS where table_name =  '{table_name}' and table_schema = '{database_name}' and column_key = 'PRI'")
        pk_columns = [row[0] for row in result]
        return tuple(pk_columns)