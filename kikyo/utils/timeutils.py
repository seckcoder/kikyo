# -*- coding: utf-8 -*-
"""
    kikyo.utils.timeutils
    ~~~~~~~~~~~~~~~~~~~~~~

    This module contains various utilities related to dates and times.

    Part of the code is from celery

"""


from kikyo.five import string_t

RATE_MODIFIER_MAP = {'s': lambda n: n,
                     'm': lambda n: n / 60.0,
                     'h': lambda n: n / 60.0 / 60.0}


def rate(rate):
    """Parses rate strings, such as `"100/m"`, `"2/h"` or `"0.5/s"`
    and converts them to seconds."""
    if rate:
        if isinstance(rate, string_t):
            ops, _, modifier = rate.partition('/')
            return RATE_MODIFIER_MAP[modifier or 's'](float(ops)) or 0
        return rate or 0
    return 0
