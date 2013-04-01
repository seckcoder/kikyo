class Producer(object):
    def __init__(self, app):
        self.app = app
    def produce(self, qkey, task_context):
        queue = self.app.queue
        queue.put(qkey, task_context)
