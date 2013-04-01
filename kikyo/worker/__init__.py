# -*- coding: utf-8 -*-

"""
    This module is inspired by celery
"""

from kikyo.utils.imports import instantiate
from kikyo.utils.functional import maybe_list
from kikyo.app import app_or_default
from .consumer import Consumer
class Worker(object):
    pool_cls =  "kikyo.concurrency.gevent:TaskPool"
    pool_concurrency = 1000
    def __init__(self, app=None, pool_cls=None, pool_concurrency=None):
        self.pool = instantiate(pool_cls or self.pool_cls,
                                limit=pool_concurrency or self.pool_concurrency)
        self.app = app_or_default(app)

        self.consumer = Consumer(callback=self.dispatch_tasks,
                                 app=self.app)
    def start(self):
        self.pool.start()
        self.consumer.start()
    def stop(self):
        self.consumer.stop()
        self.pool.stop()
    def dispatch_tasks(self, tasks):
        tasks = maybe_list(tasks)
        for t in tasks:
            target=self.app._tasks[t["name"]]
            self.pool.apply_async(target=target,
                                  args=t["args"],
                                  kwargs=t["kwargs"],
                                  callback=t["callback"])
