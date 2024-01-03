import logging
import re

from clickzetta.dbapi.connection import Connection as ClickZettaConnection
from clickzetta.client import Client
import sqlparse

from migration.connector.source.base import Source
from migration.base.exceptions import SourceExecutionError
from migration.connector.source.enum import Column, ClusterInfo
import migration.util.script_util as script_utils

logger = logging.getLogger(__name__)


class ClickzettaSource(Source):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Clickzetta', config)
        self.instance_id = None
        self.workspace = None
        self.meta_conf_path = meta_conf_path
        self.storage_config_path = storage_conf_path
        self.connection_params = self.get_connection_params()

    def get_connection_params(self):
        assert self.config.get("service"), "service is required"
        assert self.config.get("workspace"), "workspace is required"
        assert self.config.get("instance"), "instance is required"
        assert self.config.get("vcluster"), "vcluster is required"
        assert self.config.get("username"), "username is required"
        assert self.config.get("password"), "password is required"
        assert self.config.get("instanceId"), "instanceId is required"
        self.instance_id = self.config.get("instanceId")
        self.workspace = self.config.get("workspace")
        return {
            "service": self.config.get("service"),
            "workspace": self.config.get("workspace"),
            "instance": self.config.get("instance"),
            "vcluster": self.config.get("vcluster"),
            "username": self.config.get("username"),
            "password": self.config.get("password"),
        }

    def connect(self):
        if self.connection is None:
            client = Client(
                service=self.connection_params["service"],
                workspace=self.connection_params["workspace"],
                instance=self.connection_params["instance"],
                vcluster=self.connection_params["vcluster"],
                username=self.connection_params["username"],
                password=self.connection_params["password"],
            )
            self.connection = ClickZettaConnection(client=client)
            logger.info(f"Connect to Clickzetta {self.connection_params['service']} successfully")

    def get_database_names(self):
        result = self.execute_sql("show schemas")
        return [row[0] for row in result]

    def get_table_names(self, database_name):
        result = self.execute_sql(f"show tables in {database_name}")
        return [row[1] for row in result]

    def get_ddl_sql(self, database_name, table_name):
        return self.execute_sql(f"show create table {database_name}.{table_name}")[0][0]

    def get_table_cluster_info(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        cluster_columns = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*)CLUSTERED(.*)BY(.*?)\((.*?)\)(.*)(\d+)(.*)BUCKETS.*', ddl_format_sql, re.S)
        if match_result:
            for cluster_column in match_result.group(4).strip().split(','):
                cluster_columns.append(cluster_column.replace('`', '').strip())
            return ClusterInfo(int(match_result.group(6).strip()), cluster_columns)

        return None

    def get_table_partition_columns(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        partition_columns = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*?)PARTITIONED(.*?)BY(.*?)\((.*?)\).*', ddl_format_sql, re.S)
        if match_result:
            for partition_column in match_result.group(4).strip().split(','):
                partition_columns.append(partition_column.replace('`', '').strip())
            return partition_columns
        return None

    def get_primary_key(self, database_name, table_name):
        ddl_sql = self.get_ddl_sql(database_name, table_name)
        primary_keys = []
        ddl_format_sql = sqlparse.format(ddl_sql, reindent=True, keyword_case='upper')
        match_result = re.match(r'(.*?)PRIMARY(.*?)KEY(.*?)\((.*?)\).*', ddl_format_sql, re.S)
        if match_result:
            for primary_key in match_result.group(4).strip().split(','):
                primary_keys.append(primary_key.replace('`', '').strip())
            return primary_keys
        return None

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        result = self.execute_sql(f"desc {database_name}.{table_name}")
        table_columns = []
        for row in result:
            column_name = row[0]
            column_type = row[1].upper() if 'not null' not in row[1] else row[1].strip().split(' ')[0].upper()
            is_null = False if 'not null' in row[1] else True
            table_columns.append(Column(column_name, column_type, is_null, default_value=None))
        return table_columns

    def execute_sql(self, sql, bind_params=None):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql, bind_params)
            return cursor.fetchall()

        except SourceExecutionError as e:
            logger.error(f"Clickzetta connector execute sql {sql} failed, error: {e}")
            raise f"Clickzetta connector execute sql {sql} failed, error: {e}"

    def type_mapping(self):
        return {
            'TIMESTAMP': 'DATETIME',
        }

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            logger.info(f"Close Clickzetta connection successfully")

    def unload_data(self, task):
        logger.info(f"Start unload data from Clickzetta")
        temp_view_name = task.name + "_temp_view"
        self.execute_sql(f"drop materialized view if exists {temp_view_name}")
        unload_sql = f"create materialized view {temp_view_name} as select * from {task.name.split('.')[0]}.{task.name.split('.')[1]}"
        if hasattr(task, 'transform_partitions') and task.transform_partitions:
            partition_sql = " where "
            columns = task.transform_partitions[0]
            values = task.transform_partitions[1]
            for i in range(len(columns)):
                partition_sql += f"{columns[i]} = '{values[i]}' and "
            unload_sql += partition_sql[:-4]
        try:
            self.execute_sql(unload_sql)
        except BaseException as e:
            logger.error(f"Create temp view {temp_view_name} failed, error: {e}")
            self.execute_sql(f"drop materialized view if exists {temp_view_name}")
            raise f"Create temp view {temp_view_name} failed, error: {e}"
        file_paths = set()
        try:
            file_paths = script_utils.get_files_path(self.instance_id, self.workspace, temp_view_name.split('.')[0],
                                                 temp_view_name.split('.')[1], meta_conf_path=self.meta_conf_path)
        except BaseException as e:
            logger.error(f"Get object storage files path failed, error: {e}")
            self.execute_sql(f"drop materialized view if exists {temp_view_name}")
            raise f"Get object storage files path failed, error: {e}"
        logger.info(f"Unload data from Clickzetta successfully")
        self.execute_sql(f"drop materialized view if exists {temp_view_name}")
        return file_paths

    def int_type_string(self):
        return 'INT'
