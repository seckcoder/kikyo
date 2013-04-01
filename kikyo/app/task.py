"""
    Part of the code is from celery.
"""

from kikyo.five import with_metaclass, class_property 
from kikyo._state import current_app
from kikyo.utils import gen_task_name, uuid

class Context(object):
    # Task Context
    id = None
    name = None
    args = None
    kwargs = None
    callback = None
    err_callback = None

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def clear(self):
        self.__dict__.clear()

    def get(self, key, default=None):
        try:
            return getattr(self, key)
        except AttributeError:
            return default

    def __repr__(self):
        return '<Context: {0!r}>'.format(vars(self))

class TaskType(type):
    """Meta class for tasks.

    Automatically registers the task in the task registry, except
    if the `abstract` attribute is set.

    If no `name` attribute is provided, then no name is automatically
    set to the name of the module it was defined in, and the class name.

    """

    def __new__(cls, name, bases, attrs):
        new = super(TaskType, cls).__new__
        task_module = attrs.get('__module__') or '__main__'

        # - Abstract class: abstract attribute should not be inherited.
        if attrs.pop('abstract', None) or not attrs.get('autoregister', True):
            return new(cls, name, bases, attrs)

        # The 'app' attribute is now a property, with the real app located
        # in the '_app' attribute.  Previously this was a regular attribute,
        # so we should support classes defining it.
        _app1, _app2 = attrs.pop('_app', None), attrs.pop('app', None)
        app = attrs['_app'] = _app1 or _app2 or current_app

        # - Automatically generate missing/empty name.
        task_name = attrs.get('name')
        if not task_name:
            attrs['name'] = task_name = gen_task_name(app, name, task_module)

        # - Create and register class.
        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework.  There should only be one class for each task
        # name, so we always return the registered version.
        tasks = app._tasks
        if task_name not in tasks:
            tasks.register(new(cls, name, bases, attrs))
        instance = tasks[task_name]
        instance.bind(app)
        return instance.__class__

    def __repr__(cls):
        if cls._app:
            return '<class {0.__name__} of {0._app}>'.format(cls)
        if cls.__v2_compat__:
            return '<unbound {0.__name__} (v2 compatible)>'.format(cls)
        return '<unbound {0.__name__}>'.format(cls)


@with_metaclass(TaskType)
class Task(object):
    """Task base class.

    When called tasks apply the :meth:`run` method.  This method must
    be defined by all tasks (that is unless the :meth:`__call__` method
    is overridden).

    """

    #: This is the instance bound to if the task is a method of a class.
    __self__ = None

    #: The application instance associated with this task class.
    _app = None

    #: Name of the task.
    name = None

    #: If :const:`True` the task is an abstract base class.
    abstract = True

    #: Rate limit for this task type.  Examples: :const:`None` (no rate
    #: limit), `'100/s'` (hundred tasks a second), `'100/m'` (hundred tasks
    #: a minute),`'100/h'` (hundred tasks an hour)
    rate_limit = None

    #: Hard time limit.
    #: Defaults to the :setting:`CELERY_TASK_TIME_LIMIT` setting.
    time_limit = None

    #: Soft time limit.
    #: Defaults to the :setting:`CELERY_TASK_SOFT_TIME_LIMIT` setting.
    soft_time_limit = None

    #: If disabled this task won't be registered automatically.
    autoregister = True

    __bound__ = False

    from_config = []

    # - Tasks are lazily bound, so that configuration is not set
    # - until the task is actually used

    @classmethod
    def bind(self, app):
        self.__bound__ = True
        self._app = app
        conf = app.conf
        for attr_name, config_name in self.from_config:
            if getattr(self, attr_name, None) is None:
                setattr(self, attr_name, conf[config_name])

        # PeriodicTask uses this to add itself to the PeriodicTask schedule.
        self.on_bound(app)

        return app

    @classmethod
    def on_bound(self, app):
        """This method can be defined to do additional actions when the
        task class is bound to an app."""
        pass

    @classmethod
    def _get_app(self):
        if not self.__bound__ or self._app is None:
            # The app property's __set__  method is not called
            # if Task.app is set (on the class), so must bind on use.
            self.bind(current_app)
        return self._app
    app = class_property(_get_app, bind)

    def __call__(self, *args, **kwargs):
        # add self if this is a bound task
        if self.__self__ is not None:
            return self.run(self.__self__, *args, **kwargs)
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    def delay(self, *args, **kwargs):
        """Star argument version of :meth:`apply_async`.

        Does not support the extra options enabled by :meth:`apply_async`.

        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.

        :returns :class:`celery.result.AsyncResult`:

        """
        return self.apply_async(args, kwargs)

    def apply_async(self, args=None, kwargs=None,
                    task_id=None, link=None, link_error=None,
                    qkey=None,
                    **options):
        task_id = task_id or uuid()

        # add 'self' if this is a bound method.
        if self.__self__ is not None:
            args = (self.__self__, ) + tuple(args)

        _qkey = qkey or self.qkey or "default"

        self.app.producer.produce(_qkey, Context(id=task_id,
                                                 name=self.name,
                                                 args=args,
                                                 kwargs=kwargs,
                                                 callback=link,
                                                 err_callback=link_error))

    def __repr__(self):
        """`repr(task)`"""
        return '<@task: {0.name}>'.format(self)

    @property
    def __name__(self):
        return self.__class__.__name__
