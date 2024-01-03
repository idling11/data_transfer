import logging
import os
import random
from datetime import datetime
from time import sleep

import pymysql
import pymysqlpool as pymysql_pool

from migration.connector.destination.base import Destination
from clickzetta.dbapi.connection import Connection as ClickZettaConnection
from clickzetta.client import Client
from migration.base.exceptions import DestinationExecutionError, GrammarRestrictionsError
from migration.base.status import Status
import migration.util.object_storage_util as object_storage_util

logger = logging.getLogger(__name__)

MAX_GET_STATUS_TIMES = 100


class MysqlDestination(Destination):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Mysql', config)
        self.connection = None
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_conf_path = storage_conf_path
        pool_config = {'host': self.connection_params['host'], 'port': self.connection_params['port'],
                       'user': self.connection_params['user'], 'password': self.connection_params['password']}
        self.pool = pymysql_pool.ConnectionPool(size=10, maxsize=50, pre_create_num=5, name='mysql_pool', **pool_config)

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
            self.connection = pymysql.connect(**self.connection_params)
            logger.info(f"Connect to Mysql {self.connection_params['host']} successfully")

    def create_table(self, table_name, ddl: str):
        self.execute_sql(ddl)
        logger.info(f"Create table {table_name} successfully")

    def create_database(self, db_name):
        self.execute_sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        logger.info(f"Create database {db_name} successfully")

    def execute_sql(self, sql, bind_params=None):
        connection = self.pool.get_connection()
        try:
            with connection.cursor() as cur:
                cur.execute(sql, bind_params)
                result = cur.fetchall()
                return result
        except Exception as e:
            logger.error("migration Error running SQL: {}, error:{}", sql, e)
            raise DestinationExecutionError('migration Error running SQL: {}, error:{}'.format(sql, e))

        finally:
            connection.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()
        self.pool = None