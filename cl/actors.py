from __future__ import absolute_import, with_statement

import logging
import sys
import traceback

from copy import copy
from itertools import count
from operator import itemgetter

from kombu import Exchange, Queue
from kombu.utils import cached_property, kwdict, reprcall, reprkwargs
from kombu.utils.encoding import safe_repr

from cl import __version__
from cl.common import collect_replies, maybe_declare, send_reply, uuid
from cl.exceptions import clError, NoReplyError
from cl.results import AsyncResult
from cl.pools import producers


builtin_fields = {"ver": __version__}


class ActorType(type):

    def __repr__(self):
        name = self.name
        if not self.name:
            try:
                name = self.__name__
            except AttributeError:
                name = self.__class__.__name__
        return "<@actor: %s>" % (name, )


class Actor(object):
    __metaclass__ = ActorType

    AsyncResult = AsyncResult

    Error = clError
    NoReplyError = NoReplyError

    #: Actor name.
    #: Defaults to the defined class name.
    name = None

    #: Default exchange used for messages to this actor.
    exchange = None

    #: List of calling types this actor should handle.
    #: Valid types are:
    #:     * direct
    #:         Send the message directly to an agent by exact routing key.
    #:     * round-robin
    #:         Send the message to an agent by round-robin.
    #:     * scatter
    #:         Send the message to all of the agents (broadcast).
    types = ("direct", )

    #: Default timeout in seconds as a float which after
    #: we give up waiting for replies.
    default_timeout = 10.0

    #: Time in seconds as a float which after replies expires.
    reply_expires = 100.0

    #: Exchanged used for replies.
    reply_exchange = Exchange("cl.reply", "direct")

    #: Should we retry publishing messages by default?
    #: Default: NO
    retry = None

    #: Default policy used when retrying publishing messages.
    #: see :meth:`kombu.BrokerConnection.ensure` for a list
    #: of supported keys.
    retry_policy = {"max_retries": 100,
                    "interval_start": 0,
                    "interval_max": 1,
                    "interval_step": 0.2}

    #: returns the next anonymous ticket number
    #: used for identifying related logs.
    next_anon_ticket = count(1).next

    #: Additional fields added to reply messages by default.
    default_fields = {}

    #: Map of calling types and their special routing keys.
    type_to_rkey = {"rr": "__rr__",
                    "round-robin": "__rr__",
                    "scatter": "__scatter__"}

    class methods:
        pass

    def __init__(self, connection=None, id=None, name=None, exchange=None,
            logger=None, **kwargs):
        self.connection = connection
        self.id = id or uuid()
        self.name = name or self.name or self.__class__.__name__
        self.exchange = exchange or self.exchange
        self.methods = self.construct_methods()
        self.logger = logger or logging.getLogger("Actor{%s}" % self.name)
        self.type_to_queue = {"direct": self.get_direct_queue,
                              "rr": self.get_rr_queue,
                              "scatter": self.get_scatter_queue}
        if self.default_fields is None:
            self.default_fields = {}

    def construct_methods(self):
        """Instantiates the methods class of this actor."""
        return self.methods()

    def direct(self, method, args, to, nowait=False, **kwargs):
        """Call method on agent listening to ``routing_key``.

        See :method:`call_or_cast` for a full list of supported
        arguments.

        If the keyword argument `nowait` is false (default) it
        will block and return the reply.

        """
        r = self.call_or_cast(method, args, routing_key=to,
                              nowait=nowait, **kwargs)
        if not nowait:
            return r.get()

    def rr(self, method, args, nowait=False, **kwargs):
        """Call method on one of the agents in round robin.

        See :method:`call_or_cast` for a full list of supported
        arguments.

        If the keyword argument `nowait` is false (default) it
        will block and return the reply.

        """
        r = self.call_or_cast(method, args, type="round-robin",
                              nowait=nowait, **kwargs)
        if not nowait:
            return r.get()

    def scatter(self, method, args, nowait=False, **kwargs):
        """Broadcast method to all agents.

        In this context the reply limit is disabled, and the timeout
        is set to 1 by default, which means we collect all the replies
        that managed to be sent within the requested timeout.

        See :method:`call_or_cast` for a full list of supported
        arguments.

        If the keyword argument `nowait` is false (default) it
        will block and return the replies.

        """
        r = self.call_or_cast(method, args, type="scatter",
                              nowait=nowait, **kwargs)
        if not nowait:
            return r.gather()

    def call_or_cast(self, method, args={}, nowait=False, **kwargs):
        """Apply remote `method` asynchronously or synchronously depending
        on the value of `nowait`.

        :param method: The name of the remote method to perform.
        :keyword args: Dictionary of arguments for the method.
        :keyword nowait: If false the call will be block until the result
           is available and return it (default), if true the call will be
           non-blocking.
        :keyword retry: If set to true then message sending will be retried
          in the event of connection failures. Default is decided by the
          :attr:`retry` attributed.
        :keyword retry_policy: Override retry policies.
           See :attr:`retry_policy`.  This must be a dictionary, and keys will
           be merged with the default retry policy.
        :keyword timeout: Timeout to wait for replies in seconds as a float
           (**only relevant in blocking mode**).
        :keyword limit: Limit number of replies to wait for
           (**only relevant in blocking mode**).
        :keyword callback: If provided, this callback will be called for every
          reply received (**only relevant in blocking mode**).
        :keyword \*\*props: Additional message properties.
           See :meth:`kombu.Producer.publish`.

        """
        return (nowait and self.cast or self.call)(method, args, **kwargs)

    def get_queues(self):
        return [self.type_to_queue[type]() for type in self.types]

    def get_direct_queue(self):
        """Returns a unique queue that can be used to listen for messages
        to this class."""
        return Queue(self.id, self.exchange, auto_delete=True)

    def get_scatter_queue(self):
        return Queue(self.id + ".scatter", self.exchange,
                     routing_key=self.type_to_rkey["scatter"])

    def get_rr_queue(self):
        return Queue(self.exchange.name + ".rr", self.exchange,
                     routing_key=self.type_to_rkey["round-robin"])

    def get_reply_queue(self, ticket):
        return Queue(ticket, self.reply_exchange, ticket, auto_delete=True,
                     queue_arguments={
                         "x-expires": int(self.reply_expires * 1000)})

    def Consumer(self, channel, **kwargs):
        """Returns a :class:`kombu.Consumer` instance for this Actor."""
        return self.Consumer(channel, self.get_queues(),
                             callbacks=[self.on_message], **kwargs)

    def cast(self, method, args={}, before=None, retry=None,
            retry_policy=None, type=None, **props):
        """Send message to actor.  Discarding replies."""
        retry = self.retry if retry is None else retry
        body = {"class": self.name, "method": method, "args": args}
        exchange = self.exchange
        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policies.
            _retry_policy = dict(_retry_policy, **retry_policy)

        if type:
            props.setdefault("routing_key", self.type_to_rkey[type])

        with producers[self._connection].acquire(block=True) as producer:
            p = producer.publish
            conn, chan = producer.connection, producer.channel
            if before is not None:
                before(conn, chan)
            maybe_declare(exchange, chan)
            (conn.ensure(producer, p, **_retry_policy)
                    if retry else p)(body, exchange=exchange, **props)

    def call(self, method, args={}, retry=False, retry_policy=None, **props):
        """Send message to actor and return :class:`AsyncResult`."""
        ticket = uuid()
        reply_q = self.get_reply_queue(ticket)

        def before(connection, channel):
            reply_q(channel).declare()

        self.cast(method, args, before,
                  **dict(props, reply_to=ticket))
        return self.AsyncResult(ticket, self)

    def handle_cast(self, body, message):
        """Handle cast message."""
        self._DISPATCH(body)

    def handle_call(self, body, message):
        """Handle call message."""
        self.reply(message,
                   self._DISPATCH(body, ticket=message.properties["reply_to"]))

    def reply(self, req, body, **props):
        return send_reply(self._connection, self.reply_exchange,
                          req, body, **props)

    def on_message(self, body, message):
        """What to do when a message is received.

        This is a kombu consumer callback taking the standard
        ``body`` and ``message`` arguments.

        Note that if the properties of the message contains
        a value for ``reply_to`` then a proper implementation
        is expected to send a reply.

        """
        if message.properties.get("reply_to"):
            handler = self.handle_call
        else:
            handler = self.handle_cast

        # Do not ack the message if an exceptional error occurs,
        # but do ack the message if SystemExit or KeyboardInterrupt
        # is raised, as this is probably intended.
        try:
            handler(body, message)
        except Exception:
            raise
        except BaseException:
            message.ack()
            raise
        else:
            message.ack()

    def _collect_replies(self, conn, channel, ticket, *args, **kwargs):
        kwargs.setdefault("timeout", self.default_timeout)
        return collect_replies(conn, channel, self.get_reply_queue(ticket),
                               *args, **kwargs)

    def lookup_action(self, name):
        try:
            method = getattr(self.methods, name)
        except AttributeError:
            raise KeyError(name)
        if not callable(method) or name.startswith("_"):
            raise KeyError(method)
        return method

    def _DISPATCH(self, body, ticket=None):
        """Dispatch message to the appropriate method
        in :attr:`methods`, handle possible exceptions,
        and return a response suitable to be used in a reply.

        To protect from calling special methods it does not dispatch
        method names starting with underscore (``_``).

        This returns the return value or exception error
        with defaults fields in a suitable format to be used
        as a reply.

        The exceptions :exc:`SystemExit` and :exc:`KeyboardInterrupt`
        will not be handled, and will propagate.

        In the case of a successful call the return value will
        be::

            {"ok": return_value, **default_fields}

        If the method raised an exception the return value
        will be::

            {"nok": [repr exc, str traceback], **default_fields}

        :raises KeyError: if the method specified is unknown
        or is a special method (name starting with underscore).

        """
        ticket = ticket or "%%%s" % self.next_anon_ticket()
        try:
            method, args = itemgetter("method", "args")(body)
            self.logger.info("{%s} --> %s",
                             ticket, self._reprcall(method, args))
            act = self.lookup_action(method)
            r = {"ok": act(**kwdict(args or {}))}
            self.logger.info("{%s} <-- %s", ticket, reprkwargs(r))
        except Exception, exc:
            einfo = sys.exc_info()
            r = {"nok": [safe_repr(exc), self._get_traceback(einfo)]}
            self.logger.error("{%s} <-- nok=%r", ticket, exc, exc_info=einfo)
        return dict(self._default_fields, **r)

    def _get_traceback(self, exc_info):
        return "".join(traceback.format_exception(*exc_info))

    def _reprcall(self, method, args):
        return "%s.%s" % (self.name, reprcall(method, (), args))

    def bind(self, connection):
        o = copy(self)
        o.connection = connection
        return o

    def is_bound(self):
        return self.connection is not None

    def __copy__(self):
        cls, args = self.__reduce__()
        return cls(*args)

    def __reduce__(self):
        return (self.__class__, (self.connection, self.id,
                                 self.name, self.exchange))

    @property
    def _connection(self):
        if not self.is_bound():
            raise self.NotBoundError("Actor is not bound to a connection.")
        return self.connection

    @cached_property
    def _default_fields(self):
        return dict(builtin_fields, **self.default_fields)
