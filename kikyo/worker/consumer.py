# -*- coding: utf-8 -*-
"""
    This module is inspired by celery
"""

from kikyo.app import app_or_default
from kikyo.utils.threads import bgThread
from kikyo.five import Empty
class Consumer(bgThread):
    ready_queue = None
    def __init__(self, callback=None, app=None):
        self.app = app_or_default(app)
        self.callback = callback
        super(Consumer, self).__init__()
    def body(self):
        kque = self.app.queue
        try:
            tasks = kque.get(timeout=1.0)
        except Empty:
            return

        if self.callback:
            self.callback(tasks)
