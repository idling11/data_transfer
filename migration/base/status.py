from enum import Enum


class Status(Enum):
    SUCCESS = 0,
    FAILED = 1,
    ERROR = 2,
    UNKNOWN = 3


class TaskStatus(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
    INIT = "INIT"


class TaskType(Enum):
    SCHEMA_MIGRATION = "SCHEMA_MIGRATION"
    DATA_MIGRATION = "DATA_MIGRATION"
    DATA_VALIDATION = "DATA_VALIDATION"
