# -*- coding: utf-8 -*-
"""
    
    kikyo.app.registry
    ~~~~~~~~~~~~~~~~~~~

    Registry of available tasks.

    This module is inspired by celery, and part of the code is from it.

"""
from __future__ import absolute_import

import inspect

from kikyo.exceptions import NotRegistered


class TaskRegistry(dict):
    NotRegistered = NotRegistered

    def __missing__(self, key):
        raise self.NotRegistered(key)

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        self[task.name] = inspect.isclass(task) and task() or task

    def unregister(self, name):
        """Unregister task by name.

        :param name: name of the task to unregister, or a
            :class:`kikyo.task.base.Task` with a valid `name` attribute.

        :raises kikyo.exceptions.NotRegistered: if the task has not
            been registered.

        """
        try:
            self.pop(getattr(name, 'name', name))
        except KeyError:
            raise self.NotRegistered(name)
