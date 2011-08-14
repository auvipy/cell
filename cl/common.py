from __future__ import absolute_import, with_statement

import socket

from collections import deque
from itertools import count

from kombu import Consumer

from kombu.utils import gen_unique_id as uuid   # noqa

from cl.pools import producers

_declared_entities = set()


def maybe_declare(entity, channel):
    if entity not in _declared_entities:
        entity(channel).declare()
        _declared_entities.add(entity)


def drain_events(connection, *args, **kwargs):
    try:
        connection.drain_events(*args, **kwargs)
    except socket.timeout:
        pass
    except socket.error:
        raise


def itermessages(conn, channel, queue, limit=1, timeout=None, **kwargs):
    acc = deque()
    def on_message(body, message):
        acc.append((body, message))

    with Consumer(channel, [queue], callbacks=[on_reply], **kwargs):
        for i in limit and xrange(limit) or count():
            try:
                conn.drain_events(timeout=timeout)
            except socket.timeout:
                break
            else:
                try:
                    yield acc.popleft()
                except IndexError:
                    pass


def send_reply(self, conn, exchange, req, msg, **props):
    with producers[conn].acquire(block=True) as producer:
        maybe_declare(self.reply_exchange, producer.channel)
        producer.publish(msg, exchange=exchange,
            **dict({"routing_key": req.properties["reply_to"],
                    "correlation_id": req.properties.get("correlation_id")},
                    **props))


def collect_replies(conn, channel, *args, **kwargs):
    no_ack = kwargs.setdefault("no_ack", True)
    received = False
    for body, message in itermessages(conn, channel, *args, **kwargs):
        if not no_ack:
            message.ack()
        received = True
        yield body
    if received:
        channel.after_reply_message_received(queue.name)
