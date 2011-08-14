from __future__ import absolute_import, with_statement

import socket
import sys

from contextlib import nested, contextmanager
from functools import partial
from itertools import count

from kombu import Consumer
from kombu.utils import cached_property

from cl.log import LogMixin
from cl.common import drain_events


class ConsumerMixin(LogMixin):
    connect_max_retries = None

    def get_consumers(self, Consumer, channel):
        raise NotImplementedError("Subclass responsibility")

    def on_connection_revived(self):
        pass

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
        with self.Consumer() as (connection, _):
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
            with conn.channel() as channel:
                with self._consume_from(
                        *self.get_consumers(partial(Consumer, channel),
                                            channel)):
                            yield conn, channel

    @contextmanager
    def _consume_from(self, *consumers):
        with nested(*consumers) as context:
            yield context
