import logging

from migration.base import ProfileConfigError
from migration.base.status import TaskType
from migration.connector.destination.base import Destination
from migration.connector.source import Source
from migration.scheduler.scheduler import SchedulerConfig, Scheduler
from migration.scheduler.transformer import Transformer
from migration.scheduler.task.validation_task import ValidationTask
from migration.util import migration_tasks_status

logger = logging.getLogger(__name__)

DOT_SPLITTER = '.'
LEFT_BRACKET = '('


class Validation(Transformer):

    def get_migration_tasks(self):
        transform_table_list = self.get_transform_table_list()
        if not transform_table_list:
            logger.warning("No table to transform, please check your profile.yml")
            raise ProfileConfigError("No table to transform, please check your profile.yml")
        validation_tables = []

        if DOT_SPLITTER not in transform_table_list[0]:
            for db in transform_table_list:
                tables = self.source.get_table_names(db)
                for table in tables:
                    validation_tables.append(f"{db}.{table[0]}")
        else:
            for table in transform_table_list:
                if '*' in table:
                    db = table.split(DOT_SPLITTER)[0]
                    tables = self.source.get_table_names(db)
                    for result in tables:
                        validation_tables.append(f"{db}.{result[0]}")
                else:
                    validation_tables.append(table)
        logger.debug(f"validation tasks: {validation_tables}")
        logger.info(f"validation tasks count: {len(validation_tables)}")
        validation_tasks = []
        for table in validation_tables:
            task = ValidationTask(name=table, task_type=TaskType.DATA_VALIDATION, destination=self.destination,
                                  project_id=self.project_id, source=self.source)
            migration_tasks_status.init_task_status(self.destination, task)
            validation_tasks.append(task)

        return validation_tasks

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
