import logging
import re
import time
from datetime import datetime

import pymysql
import sqlparse
import pymysqlpool as pymysql_pool

from migration.connector.source.base import Source
from migration.base.exceptions import SourceExecutionError
from migration.connector.source.enum import Column, ClusterInfo
import migration.util.object_storage_util as object_storage_util

logger = logging.getLogger(__name__)


class DorisSource(Source):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Doris', config)
        self.connection_params = self.get_connection_params()
        self.meta_conf_path = meta_conf_path
        self.storage_config_path = storage_conf_path
        pool_config = {'host': self.connection_params['host'], 'port': self.connection_params['port'],
                       'user': self.connection_params['user'], 'password': self.connection_params['password']}
        self.pool = pymysql_pool.ConnectionPool(size=10, maxsize=50, pre_create_num=5, name='doris_pool', **pool_config)

    """
    Doris connection parameters is a dict including following keys:
    1. host: host of Doris
    2. port: port for mysql client, default is 9030
    3. database: database name
    4. user: user name
    5. passwd: password
    """

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

    def get_database_names(self):
        result = self.execute_sql("show databases")
        return [row[0] for row in result]

    def get_table_names(self, database_name):
        result = self.execute_sql(f"show tables from {database_name}")
        return [row[0] for row in result]

    def get_ddl_sql(self, database_name, table_name):
        return self.execute_sql(f"show create table {database_name}.{table_name}")[0][1]

    def get_table_cluster_info(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        cluster_columns = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*)DISTRIBUTED(.*)BY(.*?)\((.*?)\)(.*)BUCKETS(.*?)(\d+).*', ddl_format_sql, re.S)
        if match_result:
            for cluster_column in match_result.group(4).strip().split(','):
                cluster_columns.append(cluster_column.replace('`', '').strip())
            return ClusterInfo(int(match_result.group(7).strip()), cluster_columns)

        return None

    def get_table_partition_columns(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        partition_columns = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*?)PARTITION(.*?)BY(.*?)\((.*?)\).*', ddl_format_sql, re.S)
        if match_result:
            for partition_column in match_result.group(4).strip().split(','):
                partition_columns.append(partition_column.replace('`', '').strip())
            return partition_columns
        return None

    def get_primary_key(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        primary_keys = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*?)UNIQUE(.*?)KEY(.*?)\((.*?)\).*', ddl_format_sql, re.S)
        if match_result:
            for primary_key in match_result.group(4).strip().split(','):
                primary_keys.append(primary_key.replace('`', '').strip())
            return primary_keys
        return None

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        result = self.execute_sql(f"desc {database_name}.{table_name}")
        table_columns = []
        for row in result:
            table_columns.append(Column(name=row[0], type=row[1].upper(),
                                        is_null=True if row[2].strip() == 'Yes' else False,
                                        default_value=row[4]))
        return table_columns

    def execute_sql(self, sql, bind_params=None):
        connection = self.pool.get_connection()
        try:
            with connection.cursor() as cur:
                cur.execute(sql, bind_params)
                result = cur.fetchall()
                return result

        except SourceExecutionError as e:
            logger.error(f"Doris connector execute sql {sql} failed, error: {e}")
            raise f"Doris connector execute sql {sql} failed, error: {e}"
        finally:
            connection.close()

    def type_mapping(self):
        return {
            'DATETIME': 'TIMESTAMP',
        }

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.pool is not None:
            self.pool = None
        logger.info(f"Close connection to Doris {self.connection_params['host']} successfully")

    def unload_data(self, task):
        logger.info(f"Begin to unload data from Doris table {task.name} to object storage")
        file_path = f"{task.project_id}/{task.id}/{task.name.split('.')[1]}/"
        object_storage_conf = object_storage_util.get_object_storage_config(self.storage_config_path)
        partitions = ''
        if hasattr(task, 'transform_partitions') and task.transform_partitions:
            partitions = ' PARTITION ( '
            values = task.transform_partitions[0]
            for index, value in enumerate(values):
                if index == 0:
                    partitions += value
                else:
                    partitions += f",{value}"
            partitions += ' ) '
        job_label = f"{task.name.split('.')[0]}_{task.name.split('.')[1]}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        unload_sql = f"EXPORT TABLE {task.name}\n" \
                     f"{partitions}\n " \
                     f"TO \"s3://{object_storage_conf['bucket']}/{file_path}\" \n" \
                     f"PROPERTIES(\n" \
                     f"\"label\" = \"{job_label}\" \n" \
                     f")\n" \
                     f"WITH S3\n" \
                     f"(\n" \
                     f"\"AWS_ENDPOINT\"=\"{object_storage_conf['endpoint']}\",\n" \
                     f"\"AWS_ACCESS_KEY\"=\"{object_storage_conf['id']}\",\n" \
                     f"\"AWS_SECRET_KEY\"=\"{object_storage_conf['key']}\",\n" \
                     f"\"AWS_REGION\"=\"{object_storage_conf['region']}\"\n" \
                     f");"
        logger.info(f"Unload data from Doris {self.connection_params['host']} with sql: {unload_sql}")
        try:
            self.execute_sql(unload_sql)
        except BaseException as e:
            logger.error(f"Unload data from Doris {self.connection_params['host']} failed, error: {e}")
            raise BaseException(f"Unload data from Doris {self.connection_params['host']} failed, error: {e}")
        check_unload_status_sql = f"SHOW EXPORT FROM {task.name.split('.')[0]} WHERE LABEL = '{job_label}';"
        while True:
            result = self.execute_sql(check_unload_status_sql)
            if len(result) > 0 and result[0][2] == 'FINISHED':
                break
            time.sleep(5)
        logger.info(f"Unload data from Doris {self.connection_params['host']} to path {file_path} successfully")
        return [file_path]

    def int_type_string(self):
        return 'INT'
