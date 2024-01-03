import logging
from datetime import datetime

from migration.base.status import TaskType, TaskStatus
from migration.connector.destination.base import Destination
from migration.connector.source import Source
from migration.connector.source.enum import Column
from migration.scheduler.task import Task
from migration.util import migration_tasks_status

logger = logging.getLogger(__name__)
NUMBER_TYPE = ['BIGINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INT', 'SMALLINT', 'TINYINT']
LEFT_BRACKET = '('


class ValidationTask(Task):
    def __init__(self, name: str, task_type: TaskType, source: Source, destination: Destination, *args, **kwargs):
        super().__init__(name, task_type, *args, **kwargs)
        self.source = source
        self.destination = destination

    def run(self):
        try:
            self.status = TaskStatus.RUNNING
            if not self.check_count():
                return self
            if not self.check_number_type_statistics():
                return self
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            self.status = TaskStatus.FAILED
            logger.error(f"ValidationTask {self.name} failed to run, error: {e}")
            return self
        self.end_time = datetime.now()
        migration_tasks_status.update_task_status(self.destination, self)
        logger.info(f"ValidationTask {self.name} finished running, status: {self.status.value}")
        return self

    def check_count(self):
        sql = f"select count(*) from {self.name}"
        source_count = self.source.execute_sql(sql)[0]
        if isinstance(source_count, dict):
            source_count = [source_count['count(*)']]
        logger.info(f"source table: {self.name} count: {source_count}")
        destination_count = self.destination.execute_sql(sql)[0]
        if isinstance(destination_count, dict):
            destination_count = [destination_count['count(*)']]
        logger.info(f"destination table {self.name} count: {destination_count}")
        if source_count[0] != destination_count[0]:
            self.status = TaskStatus.FAILED
            self.end_time = datetime.now()
            migration_tasks_status.update_task_status(self.destination, self)
            logger.error(
                f"ValidationTask {self.name} failed to run, error: count not equal, retry times: {self.retry_times}")
            return False
        return True

    def check_number_type_statistics(self):
        type_mapping = self.source.type_mapping()
        table_columns = self.source.get_table_columns(self.name.split('.')[0], self.name.split('.')[1])
        for column in table_columns:
            if self.is_number_type(column, type_mapping):
                sql = f"select min({column.name}) as min_value, max({column.name}) as max_value, avg({column.name}) as avg_value from {self.name}"
                source_result = self.source.execute_sql(sql)[0]
                if isinstance(source_result, dict):
                    source_result = [source_result['min_value'], source_result['max_value'], source_result['avg_value']]
                logger.info(f"source table: {self.name} column: {column.name} statistics: {source_result}")
                destination_result = self.destination.execute_sql(sql)[0]
                if isinstance(destination_result, dict):
                    destination_result = [destination_result['min_value'], destination_result['max_value'], destination_result['avg_value']]
                logger.info(f"destination table: {self.name} column: {column.name} statistics: {destination_result}")
                if source_result[0] != destination_result[0] or source_result[1] != destination_result[1] or source_result[2] != destination_result[2]:
                    self.status = TaskStatus.FAILED
                    self.end_time = datetime.now()
                    migration_tasks_status.update_task_status(self.destination, self)
                    logger.error(
                        f'ValidationTask {self.name} failed to run, error: number type statistics '
                        f'not equal, retry times: {self.retry_times}')
                    return False
        return True

    def check_string_type_statistics(self):
        pass

    def check_top_n(self):
        pass

    def is_number_type(self, column: Column, type_mapping: dict) -> bool:
        if LEFT_BRACKET in column.type:
            column_type = column.type.split(LEFT_BRACKET)[0]
            return type_mapping.get(column_type, column_type) in NUMBER_TYPE

        return type_mapping.get(column.type, column.type) in NUMBER_TYPE

    def default_profile_sql(self, type_mapping):
        columns = self.source.get_table_columns(self.name.split('.')[0], self.name.split('.')[1])
        profile_sql = f"with source_data data as (select * from {self.name}), \n" \
                      f"column_profiles  as ( \n"
        for index, column in enumerate(columns):
            profile_sql += f"select '{column.name}' as column_name, \n" \
                           f"'{column.type}' as column_type, \n" \
                           f"count(*) as row_count, \n" \
                           f"sum(case when {column.name} is null then 0 else 1 end) / count(*) as not_null_proportion,\n" \
                           f"count(distinct {column.name}) / count(*) as distinct_proportion, \n" \
                           f"count(distinct {column.name}) as distinct_count, \n" \
                           f"count(distinct {column.name}) = count(*) as is_unique, \n"
            if self.is_number_type(column, type_mapping):
                profile_sql += f"min({column.name}) as min_value, \n" \
                               f"max({column.name}) as max_value, \n" \
                               f"avg({column.name}) as avg_value \n" \
                               f"stddev_pop({column.name}) as stddev_pop_value \n" \
                               f"stddev_sample({column.name}) as stddev_sample_value \n"
            else:
                profile_sql += f"null as min_value, \n" \
                               f"null as max_value, \n" \
                               f"null as avg_value \n" \
                               f"null as stddev_pop_value \n" \
                               f"null as stddev_sample_value \n"
            profile_sql += f"from source_data \n"
            if index != len(columns) - 1:
                profile_sql += f"union all \n"
        profile_sql += f") \n" \
                       f"select * from column_profiles;"

        return profile_sql
