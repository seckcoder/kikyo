"""
    Part of the code is from celery.
"""

import os
import sys

from uuid import UUID, uuid4 as _uuid4, _uuid_generate_random
try:
    import ctypes
except:
    ctypes = None  # noqa

#: Billiard sets this when execv is enabled.
#: We use it to find out the name of the original ``__main__``
#: module, so that we can properly rewrite the name of the
#: task to be that of ``App.main``.
MP_MAIN_FILE = os.environ.get('MP_MAIN_FILE') or None

class cached_property(object):
    """Property descriptor that caches the return value
    of the get function.

    *Examples*

    .. code-block:: python

        @cached_property
        def connection(self):
            return Connection()

        @connection.setter  # Prepares stored value
        def connection(self, value):
            if value is None:
                raise TypeError('Connection must be a connection')
            return value

        @connection.deleter
        def connection(self, value):
            # Additional action to do at del(self.attr)
            if value is not None:
                print('Connection {0!r} deleted'.format(value)

    """

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.__get = fget
        self.__set = fset
        self.__del = fdel
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__get(obj)
            return value

    def __set__(self, obj, value):
        if obj is None:
            return self
        if self.__set is not None:
            value = self.__set(obj, value)
        obj.__dict__[self.__name__] = value

    def __delete__(self, obj):
        if obj is None:
            return self
        try:
            value = obj.__dict__.pop(self.__name__)
        except KeyError:
            pass
        else:
            if self.__del is not None:
                self.__del(obj, value)

    def setter(self, fset):
        return self.__class__(self.__get, fset, self.__del)

    def deleter(self, fdel):
        return self.__class__(self.__get, self.__set, fdel)

def gen_task_name(app, name, module_name):
    try:
        module = sys.modules[module_name]
    except KeyError:
        # Fix for manage.py shell_plus (Issue #366)
        module = None

    if module is not None:
        module_name = module.__name__
        if MP_MAIN_FILE and module.__file__ == MP_MAIN_FILE:
            # - see comment about :envvar:`MP_MAIN_FILE` above.
            module_name = '__main__'
    if module_name == '__main__' and app.main:
        return '.'.join([app.main, name])
    return '.'.join(p for p in [module_name, name] if p)

def uuid4():
    # Workaround for http://bugs.python.org/issue4607
    if ctypes and _uuid_generate_random:  # pragma: no cover
        buffer = ctypes.create_string_buffer(16)
        _uuid_generate_random(buffer)
        return UUID(bytes=buffer.raw)
    return _uuid4()


def uuid():
    """Generate a unique id, having - hopefully - a very small chance of
    collision.

    For now this is provided by :func:`uuid.uuid4`.
    """
    return str(uuid4())
gen_unique_id = uuid
