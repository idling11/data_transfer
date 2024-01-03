import logging
from datetime import datetime

from migration.base.status import TaskType, TaskStatus
from migration.connector.destination.base import Destination
from migration.connector.source import Source
from migration.scheduler.task.base_task import Task
from migration.util import migration_tasks_status

logger = logging.getLogger(__name__)


class DataMigrationTask(Task):

    def __init__(self, name: str, task_type: TaskType, destination: Destination, source: Source, dest_table=None,
                 transform_partitions=None,  *args, **kwargs):
        super().__init__(name, task_type, *args, **kwargs)
        self.destination = destination
        self.source = source
        self.transform_partitions = transform_partitions
        self.dest_table = dest_table

    def run(self) -> Task:
        try:
            self.status = TaskStatus.RUNNING
            file_paths = self.source.unload_data(self)
            for file_path in file_paths:
                self.destination.load_external_data(file_path=file_path, schema_name=self.name.split(".")[0] if not self.dest_table else self.dest_table.split(".")[0],
                                                    table_name=self.name.split(".")[1] if not self.dest_table else self.dest_table.split(".")[1])
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            self.status = TaskStatus.FAILED
            self.end_time = datetime.now()
            migration_tasks_status.update_task_status(self.destination, self)
            logger.error(
                f"DataMigrationTask {self.name} failed to run, error: {e}, "
                f"try times: {self.retry_times}")
            return self
        self.end_time = datetime.now()
        migration_tasks_status.update_task_status(self.destination, self)
        logger.info(f"DataMigrationTask {self.name} finished running, status: {self.status.value}")
        return self
