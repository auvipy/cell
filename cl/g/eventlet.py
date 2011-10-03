from __future__ import absolute_import

import warnings

from eventlet import Timeout      # noqa
from eventlet import event
from eventlet import greenthread
from eventlet import queue
from greenlet import GreenletExit
from kombu import syn

blocking = syn.blocking
spawn = greenthread.spawn
Queue = queue.LightQueue
Event = event.Event


class Entry(object):
    g = None

    def __init__(self, interval, fun, *args, **kwargs):
        self.interval = interval
        self.fun = fun
        self.args = args
        self.kwargs = kwargs
        self.cancelled = False
        self._spawn()

    def _spawn(self):
        self.g = greenthread.spawn_after_local(self.interval, self)
        self.g.link(self._exit)

    def __call__(self):
        try:
            return blocking(self.fun, *self.args, **self.kwargs)
        except Exception, exc:
            warnings.warn("Periodic timer %r raised: %r" % (self.fun, exc))
        finally:
            self._spawn()

    def _exit(self, g):
        try:
            self.g.wait()
        except GreenletExit:
            self.cancel()

    def cancel(self):
        if self.g and not self.cancelled:
            self.g.cancel()
            self.cancelled = True

    def kill(self):
        if self.g:
            try:
                self.g.kill()
            except GreenletExit:
                pass

    def __repr__(self):
        return "<Entry: %r (%s)>" % (
                    self.fun, "cancelled" if self.cancelled else "alive")


def timer(interval, fun, *args, **kwargs):
    return Entry(interval, fun, *args, **kwargs)
