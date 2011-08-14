"""cl.exceptions"""
from __future__ import absolute_import

FRIENDLY_ERROR_FMT = """
Remote method raised exception:
------------------------------------
%s
"""


class clError(Exception):
    """Remote method raised exception."""
    exc = None
    traceback = None

    def __init__(self, exc=None, traceback=None):
        self.exc = exc
        self.traceback = traceback
        Exception.__init__(self, exc, traceback)

    def __str__(self):
        return FRIENDLY_ERROR_FMT % (self.traceback, )


class NoReplyError(Exception):
    """No reply received within time constraint"""
    pass


class NotBoundError(Exception):
    """Object is not bound to a connection."""
