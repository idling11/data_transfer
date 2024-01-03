import logging

import migration.util.migration_tasks_status as migration_tasks_status
from migration.base import ProfileConfigError
from migration.base.exceptions import SchedulerRuntimeError
from migration.base.status import TaskType
from migration.connector.destination.base import Destination
from migration.connector.source import Source
from migration.scheduler.scheduler import SchedulerConfig, Scheduler
from migration.scheduler.transformer import Transformer
from migration.scheduler.task.data_task import DataMigrationTask

DOT_SPLITTER = '.'
LEFT_BRACKET = '('

logger = logging.getLogger(__name__)


class DataTransformer(Transformer):
    def __init__(self, source: Source, destination: Destination, project_name: str, db_list=None,
                 config_table_list=None,
                 external_table_list=None,
                 scheduler_concurrency=1, quit_if_fail=False, thread_concurrency=1, transform_partitions=None, dest_table_list=None):
        super().__init__(source, destination, project_name, db_list, config_table_list, external_table_list,
                         scheduler_concurrency, quit_if_fail, thread_concurrency, dest_table_list)
        self.transform_partitions = transform_partitions

    def get_migration_tasks(self):
        transform_table_list = self.get_transform_table_list()
        if not transform_table_list:
            logger.warning("No table to transform, please check your profile.yml")
            raise ProfileConfigError("No table to transform, please check your profile.yml")
        data_migration_tables = []
        dest_table_list = self.get_dest_table_list()
        if dest_table_list:
            if len(dest_table_list) != len(transform_table_list):
                raise ProfileConfigError("dest_table_list length should be equal to transform_table_list length")

        if DOT_SPLITTER not in transform_table_list[0]:
            for db in transform_table_list:
                tables = self.source.get_table_names(db)
                for table in tables:
                    data_migration_tables.append(f"{db}.{table[0]}")
        else:
            for table in transform_table_list:
                if '*' in table:
                    db = table.split(DOT_SPLITTER)[0]
                    tables = self.source.get_table_names(db)
                    for result in tables:
                        data_migration_tables.append(f"{db}.{result[0]}")
                else:
                    data_migration_tables.append(table)
        logger.debug(f"data migration tasks: {data_migration_tables}")
        logger.info(f"data migration tasks count: {len(data_migration_tables)}")
        data_migration_tasks = []
        for index, table in enumerate(data_migration_tables):
            task = DataMigrationTask(name=table, task_type=TaskType.DATA_MIGRATION, destination=self.destination,
                                     project_id=self.project_id, source=self.source, dest_table=dest_table_list[index] if dest_table_list else None,
                                     transform_partitions=self.transform_partitions[
                                         table] if self.transform_partitions else None)
            migration_tasks_status.init_task_status(self.destination, task)
            data_migration_tasks.append(task)

        return data_migration_tasks

    def schedule_migration_tasks(self):
        migration_tasks = self.get_migration_tasks()
        if migration_tasks:
            if len(migration_tasks) < self.transform_concurrency:
                schedule_config = SchedulerConfig(quit_if_failed=self.quit_if_fail, max_workers=self.thread_concurrency)
                scheduler = Scheduler(schedule_config)
                self.schedules_tasks_map[scheduler] = migration_tasks
            else:
                for i in range(self.transform_concurrency):
                    schedule_config = SchedulerConfig(quit_if_failed=self.quit_if_fail, max_workers=self.thread_concurrency)
                    scheduler = Scheduler(schedule_config)
                    self.schedules_tasks_map[scheduler] = migration_tasks[i::self.transform_concurrency]
