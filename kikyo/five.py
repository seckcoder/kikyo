# -*- coding: utf-8 -*-
"""
    All of the code is from celery

    celery.five
    ~~~~~~~~~~~

    Compatibility implementations of features
    only available in newer Python versions.


"""
from __future__ import absolute_import

############## py3k #########################################################
import sys
PY3 = sys.version_info[0] == 3
import traceback

try:
    reload = reload                         # noqa
except NameError:                           # pragma: no cover
    from imp import reload                  # noqa

try:
    from UserList import UserList           # noqa
except ImportError:                         # pragma: no cover
    from collections import UserList        # noqa

try:
    from UserDict import UserDict           # noqa
except ImportError:                         # pragma: no cover
    from collections import UserDict        # noqa

if sys.platform.startswith('java'):  # pragma: no cover

    def default_encoding():
        return 'utf-8'
else:

    def default_encoding():       # noqa
        return sys.getfilesystemencoding()


# Optimized with gevent queue
from gevent.queue import Queue, Empty

if PY3:
    import builtins

    from itertools import zip_longest
    from io import StringIO, BytesIO

    map = map
    string = str
    string_t = str
    long_t = int
    text_t = str
    range = range

    open_fqdn = 'builtins.open'

    def items(d):
        return d.items()

    def keys(d):
        return d.keys()

    def values(d):
        return d.values()

    def nextfun(it):
        return it.__next__

    exec_ = getattr(builtins, 'exec')

    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

    class WhateverIO(StringIO):

        def write(self, data):
            if isinstance(data, bytes):
                data = data.encode()
            StringIO.write(self, data)

else:
    import __builtin__ as builtins  # noqa
    from itertools import imap as map, izip_longest as zip_longest  # noqa
    from StringIO import StringIO   # noqa
    string = unicode                # noqa
    string_t = basestring           # noqa
    text_t = unicode
    long_t = long                   # noqa
    range = xrange

    open_fqdn = '__builtin__.open'

    def items(d):                   # noqa
        return d.iteritems()

    def keys(d):                    # noqa
        return d.iterkeys()

    def values(d):                  # noqa
        return d.itervalues()

    def nextfun(it):                # noqa
        return it.next

    def exec_(code, globs=None, locs=None):
        """Execute code in a namespace."""
        if globs is None:
            frame = sys._getframe(1)
            globs = frame.f_globals
            if locs is None:
                locs = frame.f_locals
            del frame
        elif locs is None:
            locs = globs
        exec("""exec code in globs, locs""")

    exec_("""def reraise(tp, value, tb=None): raise tp, value, tb""")

    BytesIO = WhateverIO = StringIO         # noqa


def with_metaclass(Type, skip_attrs=set(['__dict__', '__weakref__'])):
    """Class decorator to set metaclass.

    Works with both Python 3 and Python 3 and it does not add
    an extra class in the lookup order like ``six.with_metaclass`` does
    (that is -- it copies the original class instead of using inheritance).

    """

    def _clone_with_metaclass(Class):
        attrs = dict((key, value) for key, value in items(vars(Class))
                     if key not in skip_attrs)
        return Type(Class.__name__, Class.__bases__, attrs)

    return _clone_with_metaclass

############## threading.TIMEOUT_MAX #######################################
try:
    from threading import TIMEOUT_MAX as THREAD_TIMEOUT_MAX
except ImportError:
    THREAD_TIMEOUT_MAX = 1e10  # noqa

class class_property(object):
    # NOTE(by seckcoder) : make a class property from get and set method
    def __init__(self, fget=None, fset=None):
        assert fget and isinstance(fget, classmethod)
        assert isinstance(fset, classmethod) if fset else True
        self.__get = fget
        self.__set = fset

        info = fget.__get__(object)  # just need the info attrs.
        self.__doc__ = info.__doc__
        self.__name__ = info.__name__
        self.__module__ = info.__module__

    def __get__(self, obj, type=None):
        if obj and type is None:
            type = obj.__class__
        return self.__get.__get__(obj, type)()

    def __set__(self, obj, value):
        if obj is None:
            return self
        return self.__set.__get__(obj)(value)

def _safe_str(s, errors='replace'):
    if PY3:  # pragma: no cover
        if isinstance(s, str):
            return s
        try:
            return str(s)
        except Exception as exc:
            return '<Unrepresentable {0!r}: {1!r} {2!r}>'.format(
                type(s), exc, '\n'.join(traceback.format_stack()))
    encoding = default_encoding()
    try:
        if isinstance(s, unicode):
            return s.encode(encoding, errors)
        return unicode(s, encoding, errors)
    except Exception as exc:
        return '<Unrepresentable {0!r}: {1!r} {2!r}>'.format(
            type(s), exc, '\n'.join(traceback.format_stack()))


def safe_repr(o, errors='replace'):
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)
