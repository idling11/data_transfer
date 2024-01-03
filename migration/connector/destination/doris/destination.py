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


class DorisDestination(Destination):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Doris', config)
        self.connection = None
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_conf_path = storage_conf_path
        pool_config = {'host': self.connection_params['host'], 'port': self.connection_params['port'],
                       'user': self.connection_params['user'], 'password': self.connection_params['password']}
        self.pool = pymysql_pool.ConnectionPool(size=10, maxsize=50, pre_create_num=5, name='doris_pool', **pool_config)

    def get_connection_params(self):
        assert self.config['fe_servers']
        assert self.config['user']
        assert self.config['password']

        return {
            'host': self.config['fe_servers'][0].split(':')[0],
            'port': int(self.config['fe_servers'][0].split(':')[1]),
            'user': self.config['user'],
            'password': self.config['password']
        }

    def connect(self):
        if self.connection is None:
            self.connection = pymysql.connect(**self.connection_params)
            logger.info(f"Connect to Doris {self.connection_params['host']} successfully")

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

    def load_external_data(self, file_path, schema_name, table_name):
        object_storage = object_storage_util.get_object_storage_config(self.storage_conf_path)
        label_name = f"{schema_name}_{table_name}_{datetime.now().day}_{random.randint(1, 100000000)}"
        load_sql = f"LOAD LABEL {schema_name}.{label_name}\n" \
                   f"(\n" \
                   f"DATA INFILE(\"s3://{object_storage['bucket']}/{file_path}*\")\n" \
                   f"INTO TABLE {table_name}\n" \
                   f"FORMAT AS \"parquet\"\n" \
                   f")\n" \
                   f"WITH S3\n" \
                   f"(\n" \
                   f"\"AWS_ENDPOINT\" = \"{object_storage['endpoint']}\",\n" \
                   f"\"AWS_ACCESS_KEY\" = \"{object_storage['id']}\",\n" \
                   f"\"AWS_SECRET_KEY\" = \"{object_storage['key']}\",\n" \
                   f"\"AWS_REGION\" = \"{object_storage['region']}\"\n" \
                   f")\n" \
                   f"PROPERTIES(\n" \
                   f"\"max_filter_ratio\" = \"1\"\n" \
                   f");"

        logger.info(f"Start to load data from {file_path} to {schema_name}.{table_name} with sql: {load_sql}")
        self.execute_sql(load_sql)
        current_times = 0
        while current_times < MAX_GET_STATUS_TIMES:
            if current_times == MAX_GET_STATUS_TIMES - 1:
                raise DestinationExecutionError(
                    f"doris load data from {file_path} to {schema_name}.{table_name}"
                    f" failed, get result more than 100 times.")
            check_state_sql = f"show load from {schema_name} where Label = '{label_name}';"
            state = self.execute_sql(check_state_sql)[0][2]
            if state == 'FINISHED':
                break
            elif state == 'CANCELLED' or state == 'FAILED':
                logger.error(f"doris load data from {file_path} to {schema_name}.{table_name} failed")
                raise DestinationExecutionError(
                    f"doris loading data from {file_path} to {schema_name}.{table_name} failed")
            else:
                logger.info(f"Waiting for doris S3 data loading, current state is {state}")
            sleep(5)
            current_times += 1

        logger.info(f"Doirs Load data from {file_path} to {schema_name}.{table_name} successfully")

    def gen_destination_ddl(self, db_name, table_name, columns, primary_keys, cluster_info, partition_columns):
        destination_ddl = 'CREATE TABLE IF NOT EXISTS {db}.{table} (\n'.format(db=db_name, table=table_name)
        for index, column in enumerate(columns):
            destination_ddl += '    {name} {type} {is_null} ' \
                .format(name=column.name,
                        type=column.type,
                        is_null='' if column.is_null else 'NOT NULL')
            # if column.default_value:
            #     destination_ddl += "   DEFAULT {default_value}".format(default_value=column.default_value)
            if index != len(columns) - 1:
                destination_ddl += ',\n'
            else:
                destination_ddl += '\n'

        if primary_keys:
            destination_ddl += '    UNIQUE KEY ({primary_keys}),\n'.format(primary_keys=','.join(primary_keys))
        destination_ddl += ') \n'
        if partition_columns:
            destination_ddl += 'PARTITIONED BY ({partitioned_by})\n'.format(partitioned_by=','.join(partition_columns))
        if cluster_info:
            destination_ddl += 'CLUSTERED BY ({clustered_by})\n'.format(
                clustered_by=','.join(cluster_info.cluster_keys))
            destination_ddl += 'SORTED BY ({clustered_by})\n'.format(clustered_by=','.join(cluster_info.cluster_keys))
            if cluster_info.bucket_num:
                destination_ddl += 'INTO {buckets} BUCKETS\n'.format(buckets=cluster_info.bucket_num)
            else:
                destination_ddl += 'INTO 32 BUCKETS\n'
        self.create_database(db_name)
        return destination_ddl
