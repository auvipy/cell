"""cl.utils"""

from __future__ import absolute_import

import operator
import time

from importlib import import_module
from itertools import imap, ifilter

__all__ = ["force_list", "flatten", "get_cls_by_name", "instantiate"]


def force_list(obj):
    if not hasattr(obj, "__iter__"):
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
    try:
        return it.next()
    except StopIteration:
        raise exc


def get_cls_by_name(name, aliases={}, imp=None):
    """Get class by name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        celery.concurrency.processes.TaskPool
                                    ^- class name

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> get_cls_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> get_cls_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> get_cls_by_name(TaskPool) is TaskPool
        True

    """
    if imp is None:
        imp = import_module

    if not isinstance(name, basestring):
        return name                                 # already a class

    name = aliases.get(name) or name
    module_name, _, cls_name = name.rpartition(".")
    try:
        module = imp(module_name)
    except ValueError, exc:
        raise ValueError("Couldn't import %r: %s" % (name, exc))
    return getattr(module, cls_name)


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`get_cls_by_name`.

    """
    return get_cls_by_name(name)(*args, **kwargs)


class TokenBucket(object):
    """Token Bucket Algorithm.

    See http://en.wikipedia.org/wiki/Token_Bucket
    Most of this code was stolen from an entry in the ASPN Python Cookbook:
    http://code.activestate.com/recipes/511490/

    .. admonition:: Thread safety

        This implementation may not be thread safe.

    """

    #: The rate in tokens/second that the bucket will be refilled
    fill_rate = None

    #: Maximum number of tokensin the bucket.
    capacity = 1

    #: Timestamp of the last time a token was taken out of the bucket.
    timestamp = None

    def __init__(self, fill_rate, capacity=1):
        self.capacity = float(capacity)
        self._tokens = capacity
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()

    def can_consume(self, tokens=1):
        """Returns :const:`True` if `tokens` number of tokens can be consumed
        from the bucket."""
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens=1):
        """Returns the expected time in seconds when a new token should be
        available.

        .. admonition:: Warning

            This consumes a token from the bucket.

        """
        _tokens = self._get_tokens()
        tokens = max(tokens, _tokens)
        return (tokens - _tokens) / self.fill_rate

    def _get_tokens(self):
        if self._tokens < self.capacity:
            now = time.time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
