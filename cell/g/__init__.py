from __future__ import absolute_import

from kombu.syn import detect_environment

from cell.utils import cached_property

G_NOT_FOUND = """\
cell does not currently support %r, please use one of %s\
"""


class G(object):
    map = {'eventlet': '_eventlet'}

    def spawn(self, fun, *args, **kwargs):
        return self.current.spawn(fun, *args, **kwargs)

    def timer(self, interval, fun, *args, **kwargs):
        return self.current.timer(interval, fun, *args, **kwargs)

    def blocking(self, fun, *args, **kwargs):
        return self.current.blocking(fun, *args, **kwargs)

    def Queue(self, *args, **kwargs):
        return self.current.Queue(*args, **kwargs)

    def Event(self, *args, **kwargs):
        return self.current.Event(*args, **kwargs)

    @cached_property
    def _eventlet(self):
        from . import eventlet
        return eventlet

    @cached_property
    def current(self):
        type = detect_environment()
        try:
            return getattr(self, self.map[type])
        except KeyError:
            raise KeyError(G_NOT_FOUND % (type,
                                          ', '.join(self.map.keys())))

g = G()
blocking = g.blocking
spawn = g.spawn
timer = g.timer
Queue = g.Queue
Event = g.Event
