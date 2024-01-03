from concurrent.futures import ThreadPoolExecutor

from migration.base.exceptions import SchedulerRuntimeError, ProfileConfigError
from migration.connector.source.base import Source
from migration.connector.destination.base import Destination
from migration.scheduler.scheduler import logger
from migration.util import migration_tasks_status


class Transformer:

    def __init__(self, source: Source, destination: Destination, project_name: str, db_list=None,
                 config_table_list=None,
                 external_table_list=None,
                 scheduler_concurrency=1, quit_if_fail=False, thread_concurrency=1, dest_table_list=None):
        self.source = source
        self.destination = destination
        self.pool = ThreadPoolExecutor(10)
        self.db_list = db_list
        self.project_name = project_name
        self.config_table_list = config_table_list
        self.external_table_list = external_table_list
        self.transform_concurrency = scheduler_concurrency
        self.schedules = None
        self.quit_if_fail = quit_if_fail
        self.schedules_tasks_map = {}
        self.thread_concurrency = thread_concurrency
        self.project_id = migration_tasks_status.init_status_table(self.destination, project_name)
        self.dest_table_list = dest_table_list

    def transform(self):
        self.schedule_migration_tasks()
        for schedule, tasks in self.schedules_tasks_map.items():
            self.pool.submit(self.run_concurrent, schedule, tasks)
        self.pool.shutdown(wait=True)
        self.source.close()
        self.destination.close()
        logger.info("All tasks are finished")

    def run_concurrent(self, scheduler, tasks):
        try:
            scheduler.tasks_num = len(tasks)
            scheduler.run()
            for task in tasks:
                scheduler.add_task(task)
            scheduler.finish()
        except Exception as e:
            logger.error(f"Scheduler {scheduler} failed to when transforming task, error: {e}")
            raise SchedulerRuntimeError(f"Scheduler {scheduler} failed when transforming task, error: {e}")

    def close(self):
        raise NotImplementedError

    def get_transform_db_list(self):
        return self.db_list

    def get_transform_table_list(self):
        if self.external_table_list:
            return self.external_table_list
        else:
            if self.config_table_list:
                return self.config_table_list
            else:
                if not self.db_list:
                    logger.error("database and table list are both not provided in profile.yml")
                    raise ProfileConfigError("database and table list are both not provided in profile.yml")
                return self.db_list

    def get_dest_table_list(self):
        return self.dest_table_list

    def get_migration_tasks(self):
        raise NotImplementedError

    def schedule_migration_tasks(self):
        raise NotImplementedError
