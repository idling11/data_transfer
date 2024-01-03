from datetime import datetime
from migration.base.status import TaskStatus, TaskType


class Task:
    def __init__(self, name: str, task_type: TaskType, project_id: str, *args, **kwargs):
        self.name = name
        self.task_type = task_type
        self.project_id = project_id
        self.id = self._gen_task_id()
        self.args = args
        self.kwargs = kwargs
        self.status = TaskStatus.INIT
        self.start_time = datetime.now()
        self.end_time = ""
        self.retry_times = 1
        self.status_id = None

    def init_task(self, *args, **kwargs):
        pass

    def _gen_task_id(self):
        return f"{self.name}_{self.task_type.value}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    def __str__(self):
        return self.name + '-' + self.task_type.value

    def __repr__(self):
        return f"<Task: {self.name}, Type:{self.task_type}>"

    def run(self):
        raise NotImplementedError

    def is_failed(self):
        return self.status == TaskStatus.FAILED

    def is_cancelled(self):
        return self.status == TaskStatus.CANCELLED

    def is_success(self):
        return self.status == TaskStatus.COMPLETED

    def is_running(self):
        return self.status == TaskStatus.RUNNING

    def is_queued(self):
        return self.status == TaskStatus.QUEUED

    def is_init(self):
        return self.status == TaskStatus.INIT
