"""cell.agents"""

from __future__ import absolute_import

from inspect import isclass
from operator import itemgetter
import weakref

from kombu.common import uuid, ignore_errors
from kombu.five import items, values
from kombu.log import get_logger, setup_logging
from kombu.mixins import ConsumerMixin
from kombu.utils import symbol_by_name

from .actors import Actor, ActorProxy, ACTOR_TYPE
from .utils import qualname, first_reply

__all__ = ['Agent', 'dAgent']

logger = get_logger(__name__)
debug, warn, error = logger.debug, logger.warn, logger.error


class dAgent(Actor):
    types = (ACTOR_TYPE.RR, ACTOR_TYPE.SCATTER, ACTOR_TYPE.DIRECT)
    MAX_ACTORS = 2

    class state(object):
        def __init__(self):
            self.registry = {}

        def _start_actor_consumer(self, actor):
            actor.consumer = actor.Consumer(self.connection.channel())
            actor.consumer.consume()
            self.registry[actor.id] = actor
            actor.agent = weakref.proxy(self.agent)
            actor.on_agent_ready()

        def spawn(self, cls, id, kwargs={}):
            """Add actor to the registry and start the actor's main method."""
            try:
                actor = symbol_by_name(cls)(
                    connection=self.connection, id=id, **kwargs)

                if actor.id in self.registry:
                    warn('Actor id %r already exists', actor.id)
                self._start_actor_consumer(actor)
                debug('Actor registered: %s', cls)
                return actor.id
            except Exception as exc:
                error('Cannot start actor: %r', exc, exc_info=True)

        def stop_all(self):
            self.agent.shutdown()

        def reset(self):
            debug('Resetting active actors')
            for actor in values(self.registry):
                if actor.consumer:
                    ignore_errors(self.connection, actor.consumer.cancel)
                actor.connection = self.connection
                self._start_actor_consumer(actor)

        def kill(self, actor_id):
            if actor_id not in self.registry:
                raise Actor.Next()
            else:
                actor = self.registry.pop(actor_id)
                if actor.consumer and actor.consumer.channel:
                    ignore_errors(self.connection, actor.consumer.cancel)

        def select(self, cls):
            for key, val in items(self.registry):
                if qualname(val.__class__) == cls:
                    return key
            # delegate to next agent.
            raise Actor.Next()

        def _shutdown(self, cancel=True, close=True, clear=True):
            try:
                for actor in values(self.registry):
                    if actor and actor.consumer:
                        if cancel:
                            ignore_errors(self.connection,
                                          actor.consumer.cancel)
                        if close and actor.consumer.channel:
                            ignore_errors(self.connection,
                                          actor.consumer.channel.close)
            finally:
                if clear:
                    self.registry.clear()

    def __init__(self, connection, id=None):
        self.registry = {}
        Actor.__init__(self, connection=connection, id=id, agent=self)

    def spawn_group(self, group, cls, n=1, nowait=False):
        return self.spawn(
            group, {'act_type': qualname(cls), 'number': n}, nowait)

    def spawn(self, cls, kwargs={}, nowait=False):
        """Spawn a new actor on a celery worker by sending
        a remote command to the worker.

        :param cls: the name of the :class:`~.cell.actors.Actor` class or its
                    derivative.

        :keyword kwargs: The keyword arguments to pass on to
                         actor __init__ (a :class:`dict`)

        :keyword nowait: If set to True (default) the call waits for the
                         result of spawning the actor.  if False, the spawning
                         is asynchronous.

        :returns :class:`~.cell.actors.ActorProxy`:,
            holding the id of the spawned actor.
        """

        actor_id = uuid()

        if str(qualname(cls)) == '__builtin__.unicode':
            name = cls
        else:
            name = qualname(cls)

        res = self.call('spawn', {'cls': name, 'id': actor_id,
                                  'kwargs': kwargs},
                        type=ACTOR_TYPE.RR, nowait=nowait)
        return ActorProxy(name, actor_id, res, agent=self,
                          connection=self.connection, **kwargs)

    def select(self, cls, **kwargs):
        """Get the id of already spawned actor

        :keyword actor: the name of the :class:`Actor` class
        """
        name = qualname(cls)
        id = first_reply(
            self.scatter('select', {'cls': name}, limit=1), cls)
        return ActorProxy(name, id, agent=self,
                          connection=self.connection, **kwargs)

    def kill(self, actor_id, nowait=False):
        return self.scatter('kill', {'actor_id': actor_id},
                            nowait=nowait)

    # ------------------------------------------------------------
    # Control methods. To be invoked locally
    # ------------------------------------------------------------

    def start(self):
        debug('Starting agent %s', self.id)
        consumer = self.Consumer(self.connection.channel())
        consumer.consume()
        self.state.reset()

    def stop(self):

        debug('Stopping agent %s', self.id)
        self.state._shutdown(clear=False)

    def shutdown(self):
        debug('Shutdown agent %s', self.id)
        self.state._shutdown(cancel=False)

    def process_message(self, actor, body, message):
        """Process actor message depending depending on the the worker settings.

        If greenlets are enabled in the worker, the actor message is processed
        in a greenlet from the greenlet pool,
        Otherwise, the message is processed by the same thread.
        The method is invoked from the callback `cell.actors.Actor.on_message`
         upon receiving of a message.

        :keyword actor: instance of :class:`Actor` or its derivative.
        The actor instance to process the message.

        For the full list of arguments see
        :meth:`cell.actors.Actor._on_message`.

        """
        if actor is not self and self.is_green():
            self.pool.spawn_n(actor._on_message, body, message)
        else:
            if not self.is_green() and message.properties.get('reply_to'):
                warn('Starting a blocking call (%s) on actor (%s) '
                     'when greenlets are disabled.',
                     itemgetter('method')(body), actor.__class__)
            actor._on_message(body, message)

    def is_green(self):
        return self.pool is not None and self.pool.is_green

    def get_default_scatter_limit(self):
        return None


class Agent(ConsumerMixin):
    actors = []

    def __init__(self, connection, id=None, actors=None):
        self.connection = connection
        self.id = id or uuid()
        if actors is not None:
            self.actors = actors
        self.actors = self.prepare_actors()

    def on_run(self):
        pass

    def run(self):
        self.info('Agent on behalf of [%s] starting...',
                  ', '.join(actor.name for actor in self.actors))
        self.on_run()
        super(Agent, self).run()

    def stop(self):
        pass

    def on_consume_ready(self, *args, **kwargs):
        for actor in self.actors:
            actor.on_agent_ready()

    def run_from_commandline(self, loglevel='INFO', logfile=None):
        setup_logging(loglevel, logfile)
        try:
            self.run()
        except KeyboardInterrupt:
            self.info('[Quit requested by user]')

    def _maybe_actor(self, actor):
        if isclass(actor):
            return actor(self.connection)
        return actor

    def prepare_actors(self):
        return [self._maybe_actor(actor).bind(self.connection, self)
                for actor in self.actors]

    def get_consumers(self, Consumer, channel):
        return [actor.Consumer(channel) for actor in self.actors]

    def get_default_scatter_limit(self, actor):
        return None
