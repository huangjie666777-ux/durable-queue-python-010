class QueueError(Exception):
    """Base error for invalid queue operations and durable state failures."""


class IdempotencyConflict(QueueError):
    pass


class LeaseConflict(QueueError):
    pass


class InvalidTransition(QueueError):
    pass