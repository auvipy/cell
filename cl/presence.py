"""scs.presence"""

from __future__ import absolute_import, with_statement

import logging
import warnings

from time import time, sleep
from contextlib import contextmanager

from kombu import Consumer, Exchange, Queue
from kombu.utils import cached_property

from .agents import Agent
from .consumers import ConsumerMixin
from .exceptions import NoRouteError
from .g import spawn, timer
from .pools import producers
from .utils import first_or_raise


logger = logging.getLogger("cl.presence")


class State(object):
    heartbeat_expire = 20

    def __init__(self, presence):
        self.presence = presence
        self._agents = {}
        self.handlers = {"online": self.when_online,
                         "offline": self.when_offline,
                         "heartbeat": self.when_heartbeat,
                         "wakeup": self.when_wakeup}

    def can(self, actor):
        able = set()
        for id, state in self.agents.iteritems():
            if actor in state["actors"]:
                able.add(id)
        return able

    def meta_for(self, actor):
        return self._agents["meta"][actor]

    def agents_by_meta(self, predicate, *sections):
        for agent, state in self._agents.iteritems():
            d = state["meta"]
            for i, section in enumerate(sections):
                d = d[section]
            if predicate(d):
                yield agent

    def first_agent_by_meta(self, predicate, *sections):
        for agent in self.agents_by_meta(predicate, *sections):
            return agent
        raise KeyError()

    def on_message(self, body, message):
        event = body["event"]
        self.handlers[event](**body)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("presence: Agents now: %r" % (self.agents, ))

    def when_online(self, agent=None, **kw):
        self._update_agent(agent, kw)

    def when_wakeup(self, **kw):
        self.presence.send_heartbeat()

    def when_heartbeat(self, agent=None, **kw):
        self._update_agent(agent, kw)

    def when_offline(self, agent=None, **kw):
        self._remove_agent(agent)

    def expire_agents(self):
        expired = set()
        for id, state in self._agents.iteritems():
            if time() > state["ts"] + self.heartbeat_expire:
                expired.add(id)

        for id in expired:
            self._remove_agent(id)
        return self._agents

    def _update_agent(self, agent, kw):
        self._agents[agent] = dict(kw)

    def _remove_agent(self, agent):
        self._agents.pop(agent, None)

    @property
    def agents(self):
        return self.expire_agents()


class Event(dict):
    pass


class Presence(ConsumerMixin):
    Event = Event
    State = State

    exchange = Exchange("cl.agents", type="topic", auto_delete=True)
    interval = 5
    _channel = None

    def __init__(self, agent, interval=None, on_awake=None):
        self.agent = agent
        self.state = self.State(self)
        self.interval = interval or self.interval
        self.connection = agent.connection
        self.on_awake = on_awake

    def get_queue(self):
        return Queue("cl.agents.%s" % (self.agent.id, ), self.exchange,
                     routing_key="#", auto_delete=True)

    def get_consumers(self, Consumer, channel):
        return [Consumer(self.get_queue(),
                         callbacks=[self.state.on_message], no_ack=True)]

    def create_event(self, type):
        return self.Event(agent=self.agent.id,
                          event=type,
                          actors=[actor.name for actor in self.agent.actors],
                          meta=self.meta(),
                          ts=time())

    def meta(self):
        return dict((actor.name, actor.meta) for actor in self.agent.actors)

    @contextmanager
    def extra_context(self, connection, channel):
        self.send_online()
        self.wakeup()
        sleep(1.0)
        if self.on_awake:
            self.on_awake()
        timer(self.interval, self.send_heartbeat)
        yield
        self.send_offline()

    def announce(self, event):
        agent = self.agent
        routing_key = self.agent.id
        with producers[agent.connection].acquire(block=True) as producer:
            producer.publish(event, exchange=self.exchange.name,
                                    routing_key=routing_key)

    def start(self):
        spawn(self.run)

    def send_online(self):
        return self.announce(self.create_event("online"))

    def send_heartbeat(self):
        return self.announce(self.create_event("heartbeat"))

    def send_offline(self):
        return self.announce(self.create_event("offline"))

    def wakeup(self):
        return self.announce(self.create_event("wakeup"))

    def can(self, actor):
        return self.state.can(actor)


class AwareAgent(Agent):

    def on_run(self):
        self.presence.start()

    def get_default_scatter_limit(self, actor):
        able = self.presence.can(actor)
        if not able:
            warnings.warn("Presence running, but no agents available?!?")
        return len(able) if able else None

    def on_awake(self):
        pass

    def lookup_agent(self, pred, *sections):
        return self.presence.state.first_agent_by_meta(pred, *sections)

    def lookup_agents(self, pred, *sections):
        return self.presence.state.agents_by_meta(pred, *sections)

    @cached_property
    def presence(self):
        return Presence(self, on_awake=self.on_awake)


class AwareActorMixin(object):

    def lookup(self, value):
        if self.agent:
            return self.agent.lookup_agent(lambda values: value in values,
                                           self.name, self.meta_lookup_section)


    def send_to_able(self, method, args, to=None, **kwargs):
        actor = None
        try:
            actor = self.lookup(to)
        except KeyError:
            raise NoRouteError(to)

        if actor:
            return self.send(method, args, to=actor, **kwargs)
        return first_or_raise(self.scatter(method, args, **kwargs),
                              NoRouteError())
