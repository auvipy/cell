"""cell.result"""

from __future__ import absolute_import
from __future__ import with_statement

from kombu.pools import producers

from .exceptions import CellError, NoReplyError

__all__ = ['AsyncResult']


class AsyncResult(object):
    Error = CellError
    NoReplyError = NoReplyError

    def __init__(self, ticket, actor):
        self.ticket = ticket
        self.actor = actor
        self._result = None

    def _first(self, replies):
        if replies is not None:
            replies = list(replies)
            if replies:
                return replies[0]
        raise self.NoReplyError('No reply received within time constraint')

    def result(self, **kwargs):
        if not self._result:
            self._result = self.get(**kwargs)
        return self._result

    def get(self, **kwargs):
        "What kind of arguments should be pass here"
        kwargs.setdefault('limit', 1)
        return self._first(self.gather(**kwargs))

    def gather(self, propagate=True, **kwargs):
        # mock collect_replies.
        # check to_python is invoked for every result
        # check collect_replies is called with teh exact parameters
        # test collect_replies separately
        connection = self.actor.connection
        gather = self._gather
        with producers[connection].acquire(block=True) as producer:
            for r in gather(producer.connection, producer.channel, self.ticket,
                            propagate=propagate, **kwargs):
                yield r

    def _gather(self, *args, **kwargs):
        """Generator over the results
        """
        propagate = kwargs.pop('propagate', True)
        return (self.to_python(reply, propagate=propagate)
                for reply in self.actor._collect_replies(*args, **kwargs))

    def to_python(self, reply, propagate=True):
        """Extracts the value out of the reply message.

        :param reply:
        In the case of a successful call the reply message will
        be::
            {'ok': return_value, **default_fields}

        Therefore the method returns: return_value, **default_fields

        If the method raised an exception the reply message
        will be::

            {'nok': [repr exc, str traceback], **default_fields}

        Depend on the
        :keyword propagate - if set to False. the method returns
         the error([repr exc, str traceback], **default_fields), If set to False
        :raise Error - the error contained in teh reply message (only if propagate is False)
        """
        try:
            return reply['ok']
        except KeyError:
            error = self.Error(*reply.get('nok') or ())
            if propagate:
                raise error
            return error
