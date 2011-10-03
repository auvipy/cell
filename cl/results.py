"""cl.result"""

from __future__ import absolute_import
from __future__ import with_statement

from kombu.pools import producers

from .exceptions import clError, NoReplyError

__all__ = ["AsyncResult"]


class AsyncResult(object):
    Error = clError
    NoReplyError = NoReplyError

    def __init__(self, ticket, actor):
        self.ticket = ticket
        self.actor = actor

    def _first(self, replies):
        if replies is not None:
            replies = list(replies)
            if replies:
                return replies[0]
        raise self.NoReplyError("No reply received within time constraint")

    def get(self, **kwargs):
        return self._first(self.gather(**dict(kwargs, limit=1)))

    def gather(self, propagate=True, **kwargs):
        connection = self.actor.connection
        gather = self._gather
        with producers[connection].acquire(block=True) as producer:
            for r in gather(producer.connection, producer.channel, self.ticket,
                            propagate=propagate, **kwargs):
                yield r

    def _gather(self, *args, **kwargs):
        propagate = kwargs.pop("propagate", True)
        return (self.to_python(reply, propagate=propagate)
                    for reply in self.actor._collect_replies(*args, **kwargs))

    def to_python(self, reply, propagate=True):
        try:
            return reply["ok"]
        except KeyError:
            error = self.Error(*reply.get("nok") or ())
            if propagate:
                raise error
            return error
