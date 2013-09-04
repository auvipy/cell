"""cell.exceptions"""

from __future__ import absolute_import

__all__ = ['CellError', 'Next', 'NoReplyError', 'NotBoundError']

FRIENDLY_ERROR_FMT = """
Remote method raised exception:
------------------------------------
%s
"""


class CellError(Exception):
    """Remote method raised exception."""
    exc = None
    traceback = None

    def __init__(self, exc=None, traceback=None):
        self.exc = exc
        self.traceback = traceback
        Exception.__init__(self, exc, traceback)

    def __str__(self):
        return FRIENDLY_ERROR_FMT % (self.traceback, )


class Next(Exception):
    """Used in a gather scenario to signify that no reply should be sent,
    to give another agent the chance to reply."""
    pass


class NoReplyError(Exception):
    """No reply received within time constraint"""
    pass


class NotBoundError(Exception):
    """Object is not bound to a connection."""
    pass


class NoRouteError(Exception):
    """Presence: No known route for wanted item."""
    pass


class WrongNumberOfArguments(Exception):
    """An actor call method is invoked without arguments"""
    pass
