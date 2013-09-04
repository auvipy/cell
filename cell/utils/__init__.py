"""cl.utils"""

from __future__ import absolute_import

import operator

from collections import namedtuple
from itertools import imap, ifilter

from kombu.utils import cached_property, symbol_by_name  # noqa

__all__ = ['force_list', 'flatten',
           'instantiate', 'cached_property']


def enum(**alt):
    keys, values = zip(*alt.items())
    return namedtuple('Enum', keys)(*values)


def setattr_default(obj, attr, value):
    if not hasattr(obj, attr):
        setattr(obj, attr, value)


def force_list(obj):
    if not hasattr(obj, '__iter__'):
        return [obj]
    return obj


def flatten(it):
    if it:
        try:
            return reduce(operator.add,
                          imap(force_list, ifilter(None, it)))
        except TypeError:
            return []
    return it


def first(it, default=None):
    try:
        it.next()
    except StopIteration:
        return default


def first_or_raise(it, exc):
    for reply in it:
        if not isinstance(reply, Exception):
            return reply
    raise exc


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`get_cls_by_name`.

    """
    return symbol_by_name(name)(*args, **kwargs)


def abbr(S, max, ellipsis='...'):
    if S and len(S) > max:
        return ellipsis and (S[:max - len(ellipsis)] + ellipsis) or S[:max]
    return S


def shortuuid(u):
    if '-' in u:
        return u[:u.index('-')]
    return abbr(u, 16)


def qualname(obj):  # noqa
    if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
        obj = obj.__class__
    return '%s.%s' % (obj.__module__, obj.__name__)

def first_reply(replies, key):
    try:
        return replies.next()
    except StopIteration:
        raise KeyError(key)