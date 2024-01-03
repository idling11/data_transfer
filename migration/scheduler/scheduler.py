import logging
import time
from concurrent.futures import ThreadPoolExecutor, CancelledError
import threading
from queue import Queue, Empty
import schedule
from migration.base.exceptions import TaskRuntimeError

logger = logging.getLogger(__name__)


class SchedulerConfig:
    def __init__(self, max_workers=10, queue_size=10, task_timeout=60, task_retry=3, quit_if_failed=False):
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.task_timeout = task_timeout
        self.task_retry = task_retry
        self.quit_if_failed = quit_if_failed


class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self.future_results = Queue(maxsize=config.queue_size)
        self.deamon_thread = threading.Thread(daemon=True, target=self.run_scheduler_jobs)
        self.succeed_tasks = []
        self.failed_tasks = []
        self.running_tasks = []
        self.finish_called = False
        self.tasks_num = 0
        logger.info(f"Scheduler is initialized with config: {config.__dict__}")

    def add_task(self, task):
        self.future_results.put(self.executor.submit(task.run))
        logger.info(f"Task:{task.__str__} is added to scheduler")

    def run(self):
        try:
            schedule.every(1).seconds.do(self.check_task_result)
            schedule.every(1).seconds.do(self.check_running_task_status)
            self.deamon_thread.start()
        except Exception as e:
            logger.error(f"Scheduler failed to process all tasks, error: {e}")

    def run_scheduler_jobs(self):
        try:
            while True:
                if self.finish_called:
                    break
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            logger.error(f"Scheduler failed to process all tasks, error: {e}")

    def check_task_result(self):
        if self.future_results.empty():
            return
        try:
            feature = self.future_results.get_nowait()
            if feature is not None:
                if feature.cancelled():
                    logger.error("Task is cancelled with unknown reason")
                    raise CancelledError("Task is cancelled with unknown reason")

                elif feature.done():
                    self.process_done_task_feature(feature)
                elif feature.running():
                    print("add running task")
                    self.running_tasks.append(feature)
                elif feature._state == "PENDING":
                    print("add pending task")
                    self.running_tasks.append(feature)

        except CancelledError as e:
            raise e
        except Exception as e:
            raise e

    def shutdown(self):
        self.executor.shutdown(wait=True, cancel_futures=True)
        schedule.clear()
        self.deamon_thread.join()
        for succeed_task in self.succeed_tasks:
            logger.info(f"Task:{succeed_task.__str__} is succeed")
        for failed_task in self.failed_tasks:
            logger.error(f"Task:{failed_task.__str__} is failed")

    def process_done_task_feature(self, feature):
        try:
            result = feature.result()
            if result.is_success():
                self.succeed_tasks.append(result)
            elif result.is_failed():
                if result.retry_times < self.config.task_retry:
                    logger.info(
                        f"Task:{result.__str__} is failed, begin to retry the {result.retry_times + 1} time, "
                        f"max_retry is {self.config.task_retry}")
                    result.retry_times += 1
                    self.future_results.put(self.executor.submit(result.run))
                else:
                    logger.error(f"Task:{result.__str__} is failed, max_retry is {self.config.task_retry}")
                    self.failed_tasks.append(result)
        except TaskRuntimeError as e:
            raise e

    def check_running_task_status(self):
        for task in self.running_tasks:
            if task.done():
                self.process_done_task_feature(task)
                self.running_tasks.remove(task)
                print(f"remove running task {task} from running_tasks")

    def finish(self):
        while True:
            logger.info(
                f"succeed_task_count:{self.succeed_tasks.__len__()}, "
                f"failed_task_count:{self.failed_tasks.__len__()}, total_task_count:{self.tasks_num}")
            if self.succeed_tasks.__len__() + self.failed_tasks.__len__() == self.tasks_num:
                self.finish_called = True
                self.shutdown()
                break
            else:
                logger.info(
                    f"Waiting for {len(self.running_tasks)} tasks to finish")
                time.sleep(5)
