"""cl.consumers"""

from __future__ import absolute_import, with_statement

import socket
import sys

from contextlib import nested, contextmanager
from functools import partial
from itertools import count

from kombu import Consumer

from .log import LogMixin

__all__ = ["ConsumerMixin"]


class ConsumerMixin(LogMixin):
    connect_max_retries = None

    def get_consumers(self, Consumer, channel):
        raise NotImplementedError("Subclass responsibility")

    def on_connection_revived(self):
        pass

    @contextmanager
    def extra_context(self, connection, channel):
        yield

    def run(self):
        while 1:
            try:
                self.consume(limit=None)
            except self.connection.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

    def consume(self, limit=None, timeout=None, safety_interval=1):
        elapsed = 0
        with self.Consumer() as (connection, channel):
            with self.extra_context(connection, channel):
                for i in limit and xrange(limit) or count():
                    try:
                        connection.drain_events(timeout=safety_interval)
                    except socket.timeout:
                        elapsed += safety_interval
                        if timeout and elapsed >= timeout:
                            raise socket.timeout()
                    except socket.error:
                        raise
                    else:
                        elapsed = 0

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds." % (exc, interval, ))

    @contextmanager
    def Consumer(self):
        with self.connection.clone() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   self.connect_max_retries)
            self.on_connection_revived()
            self.info("Connected to %s" % (conn.as_uri(), ))
            channel = conn.channel()
            channel = channel.__enter__()
            consumers = self.get_consumers(partial(Consumer, channel), channel)
            try:
                with self._consume_from(
                        *self.get_consumers(partial(Consumer, channel),
                                            channel)):
                            yield conn, channel
            finally:
                channel.__exit__(*sys.exc_info())

    @contextmanager
    def _consume_from(self, *consumers):
        with nested(*consumers) as context:
            yield context
