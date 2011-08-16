"""cl.utils"""

from __future__ import absolute_import

import operator

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
