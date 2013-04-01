"""
    This module is inspired by celery, part of the code is from it.
"""

UNREGISTERED_FMT = """\
Task of kind {0} is not registered, please make sure it's imported.\
"""

class NotRegistered(KeyError):
    """The task is not registered."""

    def __repr__(self):
        return UNREGISTERED_FMT.format(self)

class MaxRetriesExceededError(Exception):
    """The tasks max restart limit has been exceeded."""
