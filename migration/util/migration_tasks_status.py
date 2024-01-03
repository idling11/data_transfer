import logging

import pandas

from migration.connector.destination.base import Destination
from migration.scheduler.task.base_task import Task
from migration.base.status import Status

STATUS_SCHEMA = "cz_migration"
logger = logging.getLogger(__name__)
PK_TABLE_DML_HINT = {'hints': {'cz.sql.allow.insert.table.with.pk': 'true'}}
INCREMENTAL_INDEX = 0


def init_status_table(destination: Destination, project_name: str):
    if destination.name.lower() == "clickzetta":
        destination.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {STATUS_SCHEMA}")

        tables_in_schema = destination.execute_sql(
            f"show tables in  {STATUS_SCHEMA} where table_name like '{project_name}_%'")

        exist_status_tables = [x[1] for x in tables_in_schema]
        exist_status_tables_index = []
        for table in exist_status_tables:
            try:
                exist_status_tables_index.append(int(table.split("_")[-1]))
            except ValueError:
                raise ValueError(f"Table name {table} is not valid")
        exist_status_tables_index.sort()
        table_index = 0 if not exist_status_tables else exist_status_tables_index[-1] + 1
        table_name = f"{STATUS_SCHEMA}.{project_name}_{table_index}"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT NOT NULL,
            task_id STRING NOT NULL,
            task_name STRING NOT NULL,
            prject_id STRING NOT NULL,
            task_status STRING NOT NULL,
            task_type STRING NOT NULL,
            task_start_time TIMESTAMP NOT NULL,
            task_end_time TIMESTAMP,
            PRIMARY KEY (id)
        )
        """
        destination.execute_sql(ddl)
        return f"{project_name}_{table_index}"
    elif destination.name.lower() == "doris":
        destination.execute_sql(f"CREATE DATABASE IF NOT EXISTS {STATUS_SCHEMA}")
        tables_in_schema = destination.execute_sql(
            f"show tables in  {STATUS_SCHEMA} where table_name like '{project_name}_%'")
        exist_status_tables = [x[0] for x in tables_in_schema]
        exist_status_tables_index = []
        for table in exist_status_tables:
            try:
                exist_status_tables_index.append(int(table.split("_")[-1]))
            except ValueError:
                raise ValueError(f"Table name {table} is not valid")
        exist_status_tables_index.sort()
        table_index = 0 if not exist_status_tables else exist_status_tables_index[-1] + 1
        table_name = f"{STATUS_SCHEMA}.{project_name}_{table_index}"
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT NOT NULL,
            task_id STRING NOT NULL,
            task_name STRING NOT NULL,
            prject_id STRING NOT NULL,
            task_status STRING NOT NULL,
            task_type STRING NOT NULL,
            task_start_time DATETIME NOT NULL,
            task_end_time DATETIME
        )
        UNIQUE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 6
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        destination.execute_sql(ddl)
        return f"{project_name}_{table_index}"


def update_task_status(destination: Destination, task: Task):
    if destination.name.lower() == "clickzetta":
        destination.execute_sql(
            f"UPDATE {STATUS_SCHEMA}.{task.project_id} SET task_status = '{task.status.value}', task_end_time = cast('{task.end_time}' as timestamp) WHERE id = '{task.status_id}'",
            PK_TABLE_DML_HINT)
    elif destination.name.lower() == "doris":
        destination.execute_sql(
            f"UPDATE {STATUS_SCHEMA}.{task.project_id} SET task_status = '{task.status.value}', task_end_time = cast('{task.end_time}' as datetime) WHERE id = '{task.status_id}'")
    logger.info(f"Updated task {task.id} status to {task.status.value}")


def init_task_status(destination: Destination, task: Task):
    sql = None
    global INCREMENTAL_INDEX
    if destination.name.lower() == "clickzetta":
        sql = f"""
        INSERT INTO {STATUS_SCHEMA}.{task.project_id} (id, task_id, task_name, prject_id, task_status, task_type, task_start_time, task_end_time) 
        values ({INCREMENTAL_INDEX},'{task.id}', '{task.name}','{task.project_id}','{task.status.value}', '{task.task_type.value}', cast('{task.start_time}' as timestamp), null)
        """
        destination.execute_sql(sql, PK_TABLE_DML_HINT)
    elif destination.name.lower() == "doris":
        sql = f"""
        INSERT INTO {STATUS_SCHEMA}.{task.project_id} (id, task_id, task_name, prject_id, task_status, task_type, task_start_time, task_end_time) 
        values ('{INCREMENTAL_INDEX}','{task.id}', '{task.name}','{task.project_id}','{task.status.value}', '{task.task_type.value}', cast('{task.start_time}' as datetime), null)
        """
        destination.execute_sql(sql)
    task.status_id = INCREMENTAL_INDEX
    INCREMENTAL_INDEX += 1
    logger.info(f"Inited task {task.id} status")


def get_last_migration_report(dest: Destination, project_name: str):
    tables_in_schema = dest.execute_sql(
        f"show tables in  {STATUS_SCHEMA} where table_name like '{project_name}_%'")
    if dest.name.lower() == "clickzetta":
        exist_status_tables = [x[1] for x in tables_in_schema]
    elif dest.name.lower() == "doris":
        exist_status_tables = [x[0] for x in tables_in_schema]
    else:
        exist_status_tables = [x[0] for x in tables_in_schema]
    exist_status_tables_index = []
    for table in exist_status_tables:
        try:
            exist_status_tables_index.append(int(table.split("_")[-1]))
        except ValueError:
            raise ValueError(f"Table name {table} is not valid")
    exist_status_tables_index.sort()
    result = dest.execute_sql(f"select * from {STATUS_SCHEMA}.{project_name}_{exist_status_tables_index[-1]}")
    import openpyxl
    wb = openpyxl.Workbook(iso_dates=True)
    ws = wb.active
    ws.append(
        ["id", "task_id", "task_name", "prject_id", "task_status", "task_type", "task_start_time", "task_end_time"])
    for row in result:
        if isinstance(row[6], pandas._libs.tslibs.timestamps.Timestamp):
            row_list = list(row)
            row_list[6] = row_list[6].strftime("%Y-%m-%d %H:%M:%S")
            row_list[7] = row_list[7].strftime("%Y-%m-%d %H:%M:%S")
            ws.append(row_list)
        else:
            ws.append(row)
    wb.save(f"{project_name}_migration_report.xlsx")
    logger.info(f"Migration report saved to {project_name}_migration_report.xlsx")


def get_last_migration_status(dest: Destination, project_name: str):
    tables_in_schema = dest.execute_sql(
        f"show tables in  {STATUS_SCHEMA} where table_name like '{project_name}_%'")
    if dest.name.lower() == "clickzetta":
        exist_status_tables = [x[1] for x in tables_in_schema]
    elif dest.name.lower() == "doris":
        exist_status_tables = [x[0] for x in tables_in_schema]
    else:
        exist_status_tables = [x[0] for x in tables_in_schema]
    exist_status_tables_index = []
    for table in exist_status_tables:
        try:
            exist_status_tables_index.append(int(table.split("_")[-1]))
        except ValueError:
            raise ValueError(f"Table name {table} is not valid")
    exist_status_tables_index.sort()
    result = dest.execute_sql(f"select * from {STATUS_SCHEMA}.{project_name}_{exist_status_tables_index[-1]}")
    pandas_dict = {'id': [], 'task_id': [], 'task_name': [], 'prject_id': [], 'task_status': [], 'task_type': [],
                   'task_start_time': [], 'task_end_time': []}
    for row in result:
        pandas_dict['id'].append(row[0])
        pandas_dict['task_id'].append(row[1])
        pandas_dict['task_name'].append(row[2])
        pandas_dict['prject_id'].append(row[3])
        pandas_dict['task_status'].append(row[4])
        pandas_dict['task_type'].append(row[5])
        pandas_dict['task_start_time'].append(row[6])
        pandas_dict['task_end_time'].append(row[7])
    import pandas
    dataframe = pandas.DataFrame(pandas_dict)
    print(f"Migration status for project {project_name}:\n")
    from tabulate import tabulate
    print(tabulate(dataframe, headers='keys', tablefmt='pretty'))
