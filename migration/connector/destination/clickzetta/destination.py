import logging
import os
import subprocess
from time import sleep
from migration.connector.source.enum import Column

from migration.util import object_storage_util

from migration.connector.destination.base import Destination
from clickzetta.dbapi.connection import Connection as ClickZettaConnection
from clickzetta.client import Client
from migration.base.exceptions import DestinationExecutionError, GrammarRestrictionsError
import migration.util.script_util as script_utils

logger = logging.getLogger(__name__)


class ClickZettaDestination(Destination):
    def __init__(self, config: dict, meta_conf_path=None, storage_conf_path=None):
        super().__init__('Clickzetta', config)
        self.connection = None
        self.instance_id = None
        self.meta_conf_path = meta_conf_path
        self.storage_conf_path = storage_conf_path

    def get_connection_params(self):
        assert self.config.get("service"), "service is required"
        assert self.config.get("workspace"), "workspace is required"
        assert self.config.get("instance"), "instance is required"
        assert self.config.get("vcluster"), "vcluster is required"
        assert self.config.get("username"), "username is required"
        assert self.config.get("password"), "password is required"
        assert self.config.get("instanceId"), "instanceId is required"
        self.instance_id = self.config.get("instanceId")
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
            connection_params = self.get_connection_params()
            client = Client(
                service=connection_params["service"],
                workspace=connection_params["workspace"],
                instance=connection_params["instance"],
                vcluster=connection_params["vcluster"],
                username=connection_params["username"],
                password=connection_params["password"],
            )
            self.connection = ClickZettaConnection(client=client)

    def create_table(self, table_name, ddl: str):
        self.execute_sql(ddl)
        logger.info(f"Create table {table_name} successfully")

    def get_table_columns(self, database_name, table_name) -> list[Column]:
        result = self.execute_sql(f"desc {database_name}.{table_name}")
        table_columns = []
        for row in result:
            column_name = row[0]
            column_type = row[1].upper() if 'not null' not in row[1] else row[1].strip().split(' ')[0].upper()
            is_null = False if 'not null' in row[1] else True
            table_columns.append(Column(column_name, column_type, is_null, default_value=None))
        return table_columns

    def create_database(self, db_name):
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
        logger.info(f"Create database {db_name} successfully")

    def execute_sql(self, sql, bind_params=None):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql, bind_params)
            return cursor.fetchall()
        except Exception as e:
            logger.error("migration Error running SQL: {}, error:{}", sql, e)
            raise DestinationExecutionError('migration Error running SQL: {}, error:{}'.format(sql, e))

    def close(self):
        self.connection.close()

    def load_external_data(self, file_path, schema_name, table_name):
        meta_cmd_path = script_utils.get_meta_cmd_path()
        object_storage = object_storage_util.get_object_storage_config(self.storage_conf_path)
        object_file_path = file_path
        if object_storage['type'] == "oss":
            object_file_path = f"oss://{object_storage['bucket']}/{file_path}"
        elif object_storage['type'] == "cos":
            object_file_path = f"cos://{object_storage['bucket']}/{file_path}"
        else:
            logger.error(f"Unsupported object storage type {object_storage['type']}")
            raise DestinationExecutionError(f"Unsupported object storage type {object_storage['type']}")

        column_info = self.execute_sql(f"DESCRIBE {schema_name}.{table_name}")
        temp_table_name = f"temp_{table_name}"
        create_temp_table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{temp_table_name} (\n"
        for index, column in enumerate(column_info):
            create_temp_table_sql += f"    {column[0]} {column[1]}"
            if index < len(column_info) - 1:
                create_temp_table_sql += ",\n"
            else:
                create_temp_table_sql += "\n"
        create_temp_table_sql += (") using text options('field.delim' = '\t', 'serialization.null.format'= '\\N', "
                                  "'collection.delim'=':', 'mapkey.delim'='='); ")
        self.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name}")
        self.execute_sql(create_temp_table_sql)

        load_cmd = f"{meta_cmd_path} append-table -i {self.instance_id} -n {self.config.get('workspace')}" \
                   f",{schema_name} -t {temp_table_name} -r \"{object_file_path}\" -c {self.meta_conf_path}"
        logger.info(
            f"Clickzetta begin to load external data {object_file_path}"
            f" to {schema_name}.{table_name} with command: {load_cmd}")
        try:
            result = subprocess.run(load_cmd, shell=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, encoding="utf-8", timeout=300)
            if result.returncode != 0:
                raise DestinationExecutionError(
                    f"migration Error loading external data: {load_cmd}, error:{result.stderr}")

        except BaseException as e:
            logger.error("migration Error loading external data: {}, error:{}", load_cmd, e)
            raise DestinationExecutionError('migration Error loading external data: {}, error:{}'.format(load_cmd, e))
        rewrite_table_sql = f"INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{temp_table_name};"
        try:
            self.execute_sql(rewrite_table_sql)
            self.execute_sql(f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name}")
        except BaseException as e:
            logger.error("migration Error loading external data: {}, error:{}", rewrite_table_sql, e)
            raise DestinationExecutionError(
                'migration Error loading external data: {}, error:{}'.format(rewrite_table_sql, e))
        logger.info(f"Clickzetta Load external data {file_path} to {schema_name}.{table_name} successfully")

    def gen_destination_ddl(self, db_name, table_name, columns, primary_keys, cluster_info, partition_columns):
        if primary_keys and cluster_info:
            for primary_key, cluster_key in zip(primary_keys, cluster_info.cluster_keys):
                if primary_key != cluster_key:
                    logger.warning(f"Primary key {primary_key} is not in cluster key {cluster_info.cluster_keys}")
                    raise GrammarRestrictionsError(f"Primary key {primary_key} is not in cluster key "
                                                   f"{cluster_info.cluster_keys}")
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
            destination_ddl += '    PRIMARY KEY ({primary_keys}),\n'.format(primary_keys=','.join(primary_keys))
        destination_ddl += ') \n'
        if cluster_info:
            destination_ddl += 'CLUSTERED BY ({clustered_by})\n'.format(
                clustered_by=','.join(cluster_info.cluster_keys))
            destination_ddl += 'SORTED BY ({clustered_by})\n'.format(clustered_by=','.join(cluster_info.cluster_keys))
            if cluster_info.bucket_num:
                destination_ddl += 'INTO {buckets} BUCKETS\n'.format(buckets=cluster_info.bucket_num)
            else:
                destination_ddl += 'INTO 32 BUCKETS\n'

        if partition_columns:
            destination_ddl += 'PARTITIONED BY ({partitioned_by})\n'.format(partitioned_by=','.join(partition_columns))
        self.create_database(db_name)
        return destination_ddl
