class ProfileConfigError(BaseException):
    """Raised when profile config is invalid."""


class SourceExecutionError(BaseException):
    """Raised when source execute error."""


class GrammarRestrictionsError(BaseException):
    """Raised when grammar error."""


class SchedulerRuntimeError(BaseException):
    """Raised when scheduler run error."""


class DestinationExecutionError(BaseException):
    """Raised when destination execute error."""


class TaskRuntimeError(BaseException):
    """Raised when task run error."""
