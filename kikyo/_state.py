# -*- coding: utf-8 -*-
"""
    Code of this file is inspired by celery._state

"""

import threading

from kikyo.local import Proxy
from kikyo.utils.threads import LocalStack

default_app = None

class _TLS(threading.local):
    current_app = None

_tls = _TLS()

_task_stack = LocalStack()

def set_default_app(app):
    global default_app
    default_app = app

def get_current_app():
    if default_app is None:
        from kikyo.app import Kikyo
        set_default_app(Kikyo(
            'default',
            set_as_current=False))
    return _tls.current_app or default_app

def get_current_task():
    """Currently executing task."""
    return _task_stack.top


def get_current_worker_task():
    """Currently executing task, that was applied by the worker.

    This is used to differentiate between the actual task
    executed by the worker and any task that was called within
    a task (using ``task.__call__`` or ``task.apply``)

    """
    for task in reversed(_task_stack.stack):
        if not task.request.called_directly:
            return task

#: Proxy to current app
current_app = Proxy(get_current_app)

#: Proxy to current task.
current_task = Proxy(get_current_task)
