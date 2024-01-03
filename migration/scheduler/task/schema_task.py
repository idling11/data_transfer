import logging
from datetime import datetime

from migration.base.exceptions import DestinationExecutionError
from migration.scheduler.task.base_task import Task, TaskType, TaskStatus
from migration.connector.destination.base import Destination
import migration.util.migration_tasks_status as migration_tasks_status

logger = logging.getLogger(__name__)


class SchemaMigrationTask(Task):

    def __init__(self, name: str, task_type: TaskType, ddl: str, destination: Destination, *args, **kwargs):
        super().__init__(name, task_type, *args, **kwargs)
        self.ddl = ddl
        self.destination = destination

    def run(self) -> Task:
        try:
            self.status = TaskStatus.RUNNING
            self.destination.execute_sql(self.ddl)
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            self.status = TaskStatus.FAILED
            self.end_time = datetime.now()
            migration_tasks_status.update_task_status(self.destination, self)
            logger.error(f"SchemaMigrationTask {self.name} failed to run, error: {e}, retry times: {self.retry_times}")
            return self
        self.end_time = datetime.now()
        migration_tasks_status.update_task_status(self.destination, self)
        logger.info(f"SchemaMigrationTask {self.name} finished running, status: {self.status.value}")

        return self
