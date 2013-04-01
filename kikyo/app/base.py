#-*- coding=utf-8 -*-

"""
    Part of the code is from celery.

"""
from __future__ import absolute_import


from kikyo.local import PromiseProxy
from .registry import TaskRegistry
from kikyo.utils import cached_property
from kikyo.utils.imports import symbol_by_name

class Kikyo(object):
    def __init__(self, main=None, set_as_current=True, tasks=None, **kwargs):
        self.main = main
        self._tasks = tasks
        self.set_as_current = set_as_current
        if not isinstance(self._tasks, TaskRegistry):
            self._tasks = TaskRegistry(self._tasks or {})
        if self.set_as_current():
            self.set_current()
    def async(self, **opts):
        def inner_create_task_cls(**opts):
            def _create_task_cls(fun):
                promise = PromiseProxy(self._task_from_fun, (fun, ), opts)
                return promise
            return _create_task_cls
        return inner_create_task_cls
    def _task_from_fun(self, fun, **options):
        base = options.pop('base', None) or self.Task
        T = type(fun.__name__, (base, ), dict({
        }, **options))()
        task = self._tasks[T.name]
        task.bind(self)
        return task
    @cached_property
    def Task(self):
        return self.create_task_cls()
    def create_task_cls(self):
        return self.subclass_with_self('kikyo.app.task:Task', name='Task',
                                       attribute='_app', abstract=True)
    def subclass_with_self(self, Class, name=None, attribute='app', **kw):
        """Subclass an app-compatible class by setting its app attribute
        to be this app instance.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, e.g.
        ``class Foo: app = None``.

        :param Class: The app-compatible class to subclass.
        :keyword name: Custom name for the target class.
        :keyword attribute: Name of the attribute holding the app,
                            default is 'app'.

        """
        Class = symbol_by_name(Class)

        attrs = dict({attribute: self}, __module__=Class.__module__,
                     __doc__=Class.__doc__, **kw)

        return type(name or Class.__name__, (Class, ), attrs)
    def __repr__(self):
        return '<{0} {1}:0x{2:x}>'.format(
            type(self).__name__, self.main or '__main__', id(self))
