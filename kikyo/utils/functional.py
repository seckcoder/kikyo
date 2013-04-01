# -*- coding: utf-8 -*-
"""
    Part of the code is from celery
"""
from kikyo.five import string_t

def is_list(l):
    """Returns true if object is list-like, but not a dict or string."""
    return hasattr(l, '__iter__') and not isinstance(l, (dict, string_t))


def maybe_list(l):
    """Returns list of one element if ``l`` is a scalar."""
    return l if l is None or is_list(l) else [l]
