"""cell.actors"""
from __future__ import absolute_import, with_statement
from functools import partial

import sys
import traceback

from itertools import count
from operator import itemgetter

from kombu import Consumer, Exchange, Queue
from kombu.common import collect_replies, maybe_declare, uuid
from kombu.five import items
from kombu.log import Log
from kombu import serialization
from kombu.pools import producers
from kombu.utils import reprcall, reprkwargs, symbol_by_name
from kombu.utils.encoding import safe_repr
from kombu.utils.functional import maybe_list

from . import __version__
from . import exceptions
from cell.exceptions import WrongNumberOfArguments
from .results import AsyncResult
from .utils import cached_property, enum, shortuuid, setattr_default

__all__ = ['Actor']
BUILTIN_FIELDS = {'ver': __version__}

ACTOR_TYPE = enum(
    DIRECT='direct',
    RR='round-robin',
    SCATTER='scatter',
)


class Actor(object):
    AsyncResult = AsyncResult

    Error = exceptions.CellError
    Next = exceptions.Next
    NoReplyError = exceptions.NoReplyError
    NoRouteError = exceptions.NoRouteError
    NotBoundError = exceptions.NotBoundError

    #: Actor name.
    #: Defaults to the defined class name.
    name = None

    #: Default exchange(direct) used for messages to this actor.
    exchange = None

    #: Default routing key used if no ``to`` argument passed.
    default_routing_key = None

    #: Delivery mode: persistent or transient. Default is persistent.
    delivery_mode = 'persistent'

    #: Set to True to disable acks.
    no_ack = False

    #: List of calling types this actor should handle.
    #: Valid types are:
    #:
    #:     * direct
    #:         Send the message directly to an agent by exact routing key.
    #:     * round-robin
    #:         Send the message to an agent by round-robin.
    #:     * scatter
    #:         Send the message to all of the agents (broadcast).
    types = (ACTOR_TYPE.DIRECT, ACTOR_TYPE.SCATTER, ACTOR_TYPE.RR)

    #: Default serializer used to send messages and reply messages.
    serializer = 'json'

    #: Default timeout in seconds as a float which after
    #: we give up waiting for replies.
    default_timeout = 5.0

    #: Time in seconds as a float which after replies expires.
    reply_expires = 100.0

    #: Exchange used for replies.
    reply_exchange = Exchange('cl.reply', 'direct')

    #: Exchange used for forwarding/binding with other actors.
    outbox_exchange = None

    #: Exchange used for receiving broadcast commands for this actor type.
    _scatter_exchange = None

    #: Exchange used for round-robin commands for this actor type.
    _rr_exchange = None

    #: Should we retry publishing messages by default?
    #: Default: NO
    retry = None

    #: time-to-live for the actor before becoming Idle
    ttl = 20

    idle = 40

    #: Default policy used when retrying publishing messages.
    #: see :meth:`kombu.BrokerConnection.ensure` for a list
    #: of supported keys.
    retry_policy = {'max_retries': 100,
                    'interval_start': 0,
                    'interval_max': 1,
                    'interval_step': 0.2}

    #: returns the next anonymous ticket number
    #: used fo+r identifying related logs.
    ticket_count = count(1)

    #: Additional fields added to reply messages by default.
    default_fields = {}

    #: Map of calling types and their special routing keys.
    type_to_rkey = {'rr': '__rr__',
                    ACTOR_TYPE.RR: '__rr__',
                    ACTOR_TYPE.SCATTER: '__scatter__'}

    meta = {}
    consumer = None

    class state(object):
        """Placeholder class for actor's supported methods."""
        pass

    def __init__(self, connection=None, id=None, name=None, exchange=None,
                 logger=None, agent=None, outbox_exchange=None,
                 group_exchange=None,  **kwargs):
        self.connection = connection
        self.id = id or uuid()
        self.name = name or self.name or self.__class__.__name__
        self.outbox_exchange = outbox_exchange or self.outbox_exchange
        self.agent = agent

        if self.default_fields is None:
            self.default_fields = {}

        # - setup exchanges and queues
        self.exchange = exchange or self.get_direct_exchange()
        if group_exchange:
            self._scatter_exchange = Exchange(
                group_exchange, 'fanout', auto_delete=True)

        typemap = {
            ACTOR_TYPE.DIRECT: [self.get_direct_queue, self._inbox_direct],
            ACTOR_TYPE.RR: [self.get_rr_queue, self._inbox_rr],
            ACTOR_TYPE.SCATTER: [self.get_scatter_queue, self._inbox_scatter]
        }

        self.type_to_queue = {k: v[0] for k, v in items(typemap)}
        self.type_to_exchange = {k: v[1] for k, v in items(typemap)}

        if not self.outbox_exchange:
            self.outbox_exchange = Exchange(
                'cl.%s.output' % self.name, type='topic',
            )
        # - setup logging
        logger_name = self.name
        if self.agent:
            logger_name = '%s#%s' % (self.name, shortuuid(self.id))
        self.log = Log('!<%s>' % logger_name, logger=logger)
        self.state = self.contribute_to_state(self.construct_state())

        # actor specific initialization.
        self.construct()

    def _add_binding(self, source, routing_key='',
                     inbox_type=ACTOR_TYPE.DIRECT):
        source_exchange = Exchange(**source)
        binder = self.get_binder(inbox_type)
        maybe_declare(source_exchange, self.connection.default_channel)
        binder(exchange=source_exchange, routing_key=routing_key)

    def _remove_binding(self, source, routing_key='',
                        inbox_type=ACTOR_TYPE.DIRECT):
        source_exchange = Exchange(**source)
        unbinder = self.get_unbinder(inbox_type)
        unbinder(exchange=source_exchange, routing_key=routing_key)

    def get_binder(self, type):
        if type == ACTOR_TYPE.DIRECT:
            entity = self.type_to_queue[type]()
        elif type in self.types:
            entity = self.type_to_exchange[type]()
        else:
            raise ValueError('Unsupported type: {0}'.format(type))
        binder = entity.bind_to
        # @TODO: Declare probably should not happened here
        entity.maybe_bind(self.connection.default_channel)
        maybe_declare(entity, entity.channel)
        return binder

    def get_unbinder(self, type):
        if type == ACTOR_TYPE.DIRECT:
            entity = self.type_to_queue[type]()
            unbinder = entity.unbind_from
        else:
            entity = self.type_to_exchange[type]()
            unbinder = entity.exchange_unbind
        entity = entity.maybe_bind(self.connection.default_channel)
        # @TODO: Declare probably should not happened here
        return unbinder

    def add_binding(self, source, routing_key='',
                    inbox_type=ACTOR_TYPE.DIRECT):
        self.call('add_binding', {
            'source': source.as_dict(),
            'routing_key': routing_key,
            'inbox_type': inbox_type,
        }, type=ACTOR_TYPE.DIRECT)

    def remove_binding(self, source, routing_key='',
                       inbox_type=ACTOR_TYPE.DIRECT):
        self.call('remove_binding', {
            'source': source.as_dict(),
            'routing_key': routing_key,
            'inbox_type':  inbox_type,
        }, type=ACTOR_TYPE.DIRECT)

    def construct(self):
        """Actor specific initialization."""
        pass

    def construct_state(self):
        """Instantiates the state class of this actor."""
        return self.state()

    def on_agent_ready(self):
        pass

    def contribute_to_object(self, obj, map):
        for attr, value in items(map):
            setattr_default(obj, attr, value)
        return obj

    def contribute_to_state(self, state):
        try:
            contribute = state.contribute_to_state
        except AttributeError:
            # set default state attributes.
            return self.contribute_to_object(state, {
                'actor': self,
                'agent': self.agent,
                'connection': self.connection,
                'log': self.log,
                'Next': self.Next,
                'NoRouteError': self.NoRouteError,
                'NoReplyError': self.NoReplyError,
                'add_binding': self._add_binding,
                'remove_binding': self._remove_binding,
            })
        else:
            return contribute(self)

    def send(self, method, args={}, to=None, nowait=False, **kwargs):
        """Call method on agent listening to ``routing_key``.

        See :meth:`call_or_cast` for a full list of supported
        arguments.

        If the keyword argument `nowait` is false (default) it
        will block and return the reply.
j
        """

        if to is None:
            to = self.routing_key
        r = self.call_or_cast(method, args, routing_key=to,
                              nowait=nowait, **kwargs)
        if not nowait:
            return r.get()

    def throw(self, method, args={}, nowait=False, **kwargs):
        """Call method on one of the agents in round robin.

        See :meth:`call_or_cast` for a full list of supported
        arguments.

        If the keyword argument `nowait` is false (default) it
        will block and return the reply.

        """
        r = self.call_or_cast(method, args, type=ACTOR_TYPE.RR,
                              nowait=nowait, **kwargs)
        if not nowait:
            return r

    def scatter(self, method, args={}, nowait=False, timeout=None, **kwargs):
        """Broadcast method to all agents.

        if nowait is False, returns generator to iterate over the results.

        :keyword limit: Limit number of reads from the queue.
            Unlimited by default.
        :keyword timeout: the timeout (in float seconds) waiting for replies.
            Default is :attr:`default_timeout`.

         **Examples**

        ``scatter`` is a generator (if nowait is False)::
            >>> res = scatter()
            >>> res.next() # one event consumed, or timed out.

            >>> res = scatter(limit=2):
            >>> for i in res:  # two events consumed or timeout
            >>>     pass

        See :meth:`call_or_cast` for a full list of supported
        arguments.

        """
        timeout = timeout if timeout is not None else self.default_timeout
        r = self.call_or_cast(method, args, type=ACTOR_TYPE.SCATTER,
                              nowait=nowait, timeout=timeout, **kwargs)
        if not nowait:
            return r.gather(timeout=timeout, **kwargs)

    def call_or_cast(self, method, args={}, nowait=False, **kwargs):
        """Apply remote `method` asynchronously or synchronously depending
        on the value of `nowait`.

        :param method: The name of the remote method to perform.
        :param args: Dictionary of arguments for the method.
        :keyword nowait: If false the call will block until the result
           is available and return it (default), if true the call will be
           non-blocking and no result will be returned.
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

    def get_scatter_exchange(self):
        """Returns a :class:'kombu.Exchange' for type fanout"""
        return Exchange('cl.scatter.%s' % self.name, 'fanout',
                        auto_delete=True)

    def get_rr_exchange(self):
        """Returns a :class:'kombu.Exchange' instance with type set to fanout.
         The exchange is used for sending in a round-robin style"""
        return Exchange('cl.rr.%s' % self.name, 'fanout', auto_delete=True)

    def get_direct_exchange(self):
        """Returns a :class:'kombu.Exchange' with type direct"""
        return Exchange('cl.%s' % self.name, 'direct', auto_delete=True)

    def get_queues(self):
        return [self.type_to_queue[type]() for type in self.types]

    def get_direct_queue(self):
        """Returns a :class: `kombu.Queue` instance to be used to listen
         for messages send to this specific Actor instance"""
        return Queue(self.id, self.inbox_direct, routing_key=self.routing_key,
                     auto_delete=True)

    def get_scatter_queue(self):
        """Returns a :class: `kombu.Queue` instance for receiving broadcast
        commands for this actor type."""
        return Queue('%s.%s.scatter' % (self.name, self.id),
                     self.inbox_scatter, auto_delete=True)

    def get_rr_queue(self):
        """Returns a :class: `kombu.Queue` instance for receiving round-robin
        commands for this actor type."""
        return Queue(self.inbox_rr.name + '.rr', self.inbox_rr,
                     auto_delete=True)

    def get_reply_queue(self, ticket):
        return Queue(ticket, self.reply_exchange, ticket, auto_delete=True,
                     queue_arguments={
                         'x-expires': int(self.reply_expires * 1000)})

    def Consumer(self, channel, **kwargs):
        """Returns a :class:`kombu.Consumer` instance for this Actor"""
        kwargs.setdefault('no_ack', self.no_ack)
        return Consumer(channel, self.get_queues(),
                        callbacks=[self.on_message], **kwargs)

    def emit(self, method, args={}, retry=None):
        return self.cast(method, args, retry=retry, exchange=self.outbox)

    def cast(self, method, args={}, declare=None, retry=None,
             retry_policy=None, type=None, exchange=None, **props):
        """Send message to actor.  Discarding replies."""
        retry = self.retry if retry is None else retry
        body = {'class': self.name, 'method': method, 'args': args}

        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policies.
            _retry_policy = dict(_retry_policy, **retry_policy)

        if type and type not in self.types:
            raise ValueError('Unsupported type: {0}'.format(type))
        elif not type:
            type = ACTOR_TYPE.DIRECT

        props.setdefault('routing_key', self.routing_key)
        props.setdefault('serializer', self.serializer)
        exchange = exchange or self.type_to_exchange[type]()
        declare = (maybe_list(declare) or []) + [exchange]
        with producers[self._connection].acquire(block=True) as producer:
            return producer.publish(body, exchange=exchange, declare=declare,
                                    retry=retry, retry_policy=retry_policy,
                                    **props)

    def call(self, method, args={}, retry=False, retry_policy=None,
             ticket=None, **props):
        """Send message to the same actor and return :class:`AsyncResult`."""
        ticket = ticket or uuid()
        reply_q = self.get_reply_queue(ticket)
        self.cast(method, args, declare=[reply_q], reply_to=ticket, **props)
        return self.AsyncResult(ticket, self)

    def handle_cast(self, body, message):
        """Handle cast message."""
        self._DISPATCH(body)

    def handle_call(self, body, message):
        """Handle call message."""
        try:
            r = self._DISPATCH(body, ticket=message.properties['reply_to'])
        except self.Next:
            # don't reply, delegate to other agents.
            pass
        else:
            self.reply(message, r)

    def reply(self, req, body, **props):
        with producers[self._connection].acquire(block=True) as producer:
            content_type = req.content_type
            serializer = serialization.registry.type_to_name[content_type]
            return producer.publish(
                body,
                declare=[self.reply_exchange],
                routing_key=req.properties['reply_to'],
                correlation_id=req.properties.get('correlation_id'),
                serializer=serializer,
                **props
            )

    def on_message(self, body, message):
        self.agent.process_message(self, body, message)

    def _on_message(self, body, message):
        """What to do when a message is received.

        This is a kombu consumer callback taking the standard
        ``body`` and ``message`` arguments.

        Note that if the properties of the message contains
        a value for ``reply_to`` then a proper implementation
        is expected to send a reply.

        """
        if message.properties.get('reply_to'):
            handler = self.handle_call
        else:
            handler = self.handle_cast

        def handle():
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
        handle()

    def _collect_replies(self, conn, channel, ticket, *args, **kwargs):
        kwargs.setdefault('timeout', self.default_timeout)

        if 'limit' not in kwargs and self.agent:
            kwargs['limit'] = self.agent.get_default_scatter_limit()
        if 'ignore_timeout' not in kwargs and not kwargs.get('limit', None):
            kwargs.setdefault('ignore_timeout', False)

        return collect_replies(conn, channel, self.get_reply_queue(ticket),
                               *args, **kwargs)

    def lookup_action(self, name):
        try:
            if not name:
                method = self.default_receive
            else:
                method = getattr(self.state, name)
        except AttributeError:
            raise KeyError(name)
        if not callable(method) or name.startswith('_'):
            raise KeyError(method)
        return method

    def default_receive(self, msg_body):
        """Override in the derived classes."""
        pass

    def _DISPATCH(self, body, ticket=None):
        """Dispatch message to the appropriate method
        in :attr:`state`, handle possible exceptions,
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

            {'ok': return_value, **default_fields}

        If the method raised an exception the return value
        will be::

            {'nok': [repr exc, str traceback], **default_fields}

        :raises KeyError: if the method specified is unknown
        or is a special method (name starting with underscore).

        """
        if ticket:
            sticket = '%s' % (shortuuid(ticket), )
        else:
            ticket = sticket = str(next(self.ticket_counter))
        try:
            method, args = itemgetter('method', 'args')(body)
            self.log.info('#%s --> %s',
                          sticket, self._reprcall(method, args))
            act = self.lookup_action(method)
            r = {'ok': act(args or {})}
            self.log.info('#%s <-- %s', sticket, reprkwargs(r))
        except self.Next:
            raise
        except Exception as exc:
            einfo = sys.exc_info()
            r = {'nok': [safe_repr(exc), self._get_traceback(einfo)]}
            self.log.error('#%s <-- nok=%r', sticket, exc)
        return dict(self._default_fields, **r)

    def _get_traceback(self, exc_info):
        return ''.join(traceback.format_exception(*exc_info))

    def _reprcall(self, method, args):
        return '%s.%s' % (self.name, reprcall(method, (), args))

    def bind(self, connection, agent=None):
        return self.__class__(connection, self.id,
                              self.name, self.exchange, agent=agent)

    def is_bound(self):
        return self.connection is not None

    def __copy__(self):
        cls, args = self.__reduce__()
        return cls(*args)

    def __reduce__(self):
        return (self.__class__, (self.connection, self.id,
                                 self.name, self.exchange))

    @property
    def outbox(self):
        return self.outbox_exchange

    def _inbox_rr(self):
        if not self._rr_exchange:
            self._rr_exchange = self.get_rr_exchange()
        return self._rr_exchange

    @property
    def inbox_rr(self):
        return self._inbox_rr()

    def _inbox_direct(self):
        return self.exchange

    @property
    def inbox_direct(self):
        return self._inbox_direct()

    def _inbox_scatter(self):
        if not self._scatter_exchange:
            self._scatter_exchange = self.get_scatter_exchange()
        return self._scatter_exchange

    @property
    def inbox_scatter(self):
        return self._inbox_scatter()

    @property
    def _connection(self):
        if not self.is_bound():
            raise self.NotBoundError('Actor is not bound to any connection.')
        return self.connection

    @cached_property
    def _default_fields(self):
        return dict(BUILTIN_FIELDS, **self.default_fields)

    @property
    def routing_key(self):
        if self.default_routing_key:
            return self.default_routing_key
        else:
            return self.id


class ActorProxy(object):
    """An actor wrapper that represents an actor started remotely.

        ActroProxy is created as a result of spawning an Actor.
        The :py:meth:`~agents.dAgent.spawn` returns an instance of ActorProxy

    *Examples*

    .. code-block:: python

        class GreetingActor(Actor):
            class state:
                def greet(who = 'world'):
                    print 'hello %s' %who

        gr_proxy = agent.spawns(GreetingActor)

        # All of the below are valid calls for gr_proxy
        gr_proxy.call.greet()
        gr_proxy.call.greet({'who':'Foo'})
        gr_proxy.throw.greet()
        gr_proxy.scatter.greet()
    """

    def __init__(self, name, id, async_start_result=None, **kwargs):
        kwargs.update({'id': id})
        self._actor = symbol_by_name(name)(**kwargs)
        self.id = self._actor.id
        self.async_start_result = async_start_result

    class state(object):
        def __init__(self, parent, id, func):
            self.parent = parent
            self.id = id
            self.func = func

        def __call__(self, *args, **kw):
            if not args:
                raise WrongNumberOfArguments(
                    'No arguments given to %s' % self.func)
            try:
                meth = getattr(self.parent.state, args[0]).__name__
            except AttributeError:
                if kw.get('typed', True):
                    raise
                else:
                    meth = args[0]
            return self.func(meth, *args[1:], **kw)

        def __getattr__(self, name):
                return partial(
                    self.func, getattr(self.parent.state, name).__name__)

    @cached_property
    def call(self):
        return self.state(self._actor, self.id, self._actor.call)

    @cached_property
    def throw(self):
            return self.state(self._actor, self.id, self._actor.throw)

    @cached_property
    def send(self):
        return self.state(self._actor, self.id, self._actor.send)

    @cached_property
    def scatter(self):
            return self.state(self._actor, self.id, self._actor.scatter)

    def __getattr__(self, name):
            return getattr(self._actor, name)

    # Notify when the actor is started
    def wait_to_start(self, **kwargs):
        return self.async_start_result.result(**kwargs)
