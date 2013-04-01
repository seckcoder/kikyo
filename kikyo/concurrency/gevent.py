# -*- coding: utf-8 -*-
"""
    kikyo.concurrency.gevent
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    gevent pool implementation.
    
    This module is inspired by celery
"""

from __future__ import absolute_import

import os
PATCHED = [0]
if not os.environ.get('GEVENT_NOPATCH') and not PATCHED[0]:
    PATCHED[0] += 1
    from gevent import monkey, version_info
    monkey.patch_all()
    if version_info[0] == 0:
        # Signals are not working along gevent in version prior 1.0
        # and they are not monkey patch by monkey.patch_all()
        from gevent import signal as _gevent_signal
        _signal = __import__('signal')
        _signal.signal = _gevent_signal

from .base import BasePool

try:
    from gevent import Timeout
except ImportError:
    Timeout = None  # noqa

from gevent.queue import Queue

def apply_timeout(target, args=(), kwargs={}, callback=None,
                  accept_callback=None, pid=None, timeout=None,
                  timeout_callback=None, **rest):
    try:
        with Timeout(timeout):
            return apply_target(target, args, kwargs, callback,
                                accept_callback, pid, **rest)
    except Timeout:
        return timeout_callback(False, timeout)

class TaskPool(BasePool):
    Queue = Queue
    signal_safe = False
    rlimit_safe = False
    is_green = True

    def __init__(self, *args, **kwargs):
        from gevent import spawn_raw
        from gevent.pool import Pool
        self.Pool = Pool
        self.spawn_n = spawn_raw
        self.timeout = kwargs.get('timeout')
        super(TaskPool, self).__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)
        self._quick_put = self._pool.spawn

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
                 accept_callback=None, timeout=None,
                 timeout_callback=None, **_):
        timeout = self.timeout if timeout is None else timeout
        return self._quick_put(apply_timeout if timeout else apply_target,
                               target, args, kwargs, callback, accept_callback,
                               timeout=timeout,
                               timeout_callback=timeout_callback)

    def grow(self, n=1):
        self._pool._semaphore.counter += n
        self._pool.size += n

    def shrink(self, n=1):
        self._pool._semaphore.counter -= n
        self._pool.size -= n

    @property
    def num_processes(self):
        return len(self._pool)
