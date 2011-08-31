"""cl.common"""

from __future__ import absolute_import, with_statement

import os
import socket
import sys

from collections import deque
from functools import partial
from itertools import count

from kombu import Consumer
from kombu import serialization
from kombu.utils import gen_unique_id as uuid   # noqa

from .log import Log
from .pools import producers

__all__ = ["maybe_declare", "itermessages", "send_reply", "collect_replies"]
_declared_entities = set()
insured_logger = Log("cl.insured")



def maybe_declare(entity, channel):
    if entity not in _declared_entities:
        entity(channel).declare()
        _declared_entities.add(entity)


def itermessages(conn, channel, queue, limit=1, timeout=None, **kwargs):
    acc = deque()

    def on_message(body, message):
        acc.append((body, message))

    with Consumer(channel, [queue], callbacks=[on_message], **kwargs):
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

def isend_reply(pool, exchange, req, msg, props, **retry_policy):
    return ipublish(pool, send_reply,
                    (exchange, req, msg), props, **retry_policy)

def send_reply(exchange, req, msg, producer=None, **props):
    content_type = req.content_type
    serializer = serialization.registry.type_to_name[content_type]
    maybe_declare(exchange, producer.channel)
    producer.publish(msg, exchange=exchange,
            **dict({"routing_key": req.properties["reply_to"],
                    "correlation_id": req.properties.get("correlation_id"),
                    "serializer": serializer},
                    **props))


def collect_replies(conn, channel, queue, *args, **kwargs):
    no_ack = kwargs.setdefault("no_ack", True)
    received = False
    for body, message in itermessages(conn, channel, queue, *args, **kwargs):
        if not no_ack:
            message.ack()
        received = True
        yield body
    if received:
        channel.after_reply_message_received(queue.name)


def _ensure_errback(exc, interval):
    insured_logger.error(
        "Connection error: %r. Retry in %ss\n" % (exc, interval),
            exc_info=sys.exc_info())


def revive_connection(connection, channel, on_revive=None):
    if getattr(connection, "_producer_chan", None):
        try:
            connection._producer_chan.close()
        except Exception:
            pass
    connection._producer_chan = channel
    if on_revive:
        on_revive(channel)


def revive_producer(producer, channel, on_revive=None):
    revive_connection(producer.connection, channel)
    if on_revive:
        on_revive(channel)


def insured(pool, fun, args, kwargs, errback=None, on_revive=None, **opts):
    """Ensures function performing broker commands completes
    despite intermittent connection failures."""
    errback = errback or _ensure_errback

    with pool.acquire(block=True) as conn:
        conn.ensure_connection(errback=errback)
        # we cache the channel for subsequent calls, this has to be
        # reset on revival.
        channel = getattr(conn, "_producer_chan", None)
        if channel is None:
            channel = conn._producer_chan = conn.channel()

        revive = partial(revive_connection, conn, on_revive=on_revive)
        insured = conn.autoretry(fun, channel, errback=errback,
                                 on_revive=revive, **opts)
        retval, _ = insured(*args, **dict(kwargs, connection=conn))
        return retval


def ipublish(pool, fun, args=(), kwargs={}, errback=None, on_revive=None,
        **retry_policy):

    with pool.acquire(block=True) as producer:
        errback = errback or _ensure_errback
        revive = partial(revive_producer, producer, on_revive=on_revive)
        f = producer.connection.ensure(producer, fun, on_revive=revive,
                                       errback=errback, **retry_policy)
        return f(*args, **dict(kwargs, producer=producer))
