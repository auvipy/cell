"""scs.presence"""

from __future__ import absolute_import, with_statement

import warnings

from time import time, sleep
from contextlib import contextmanager

from kombu import Consumer, Exchange, Queue
from kombu.utils import cached_property

from .agents import Agent
from .consumers import ConsumerMixin
from .pools import producers


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

    def on_message(self, body, message):
        event = body["event"]
        self.handlers[event](**body)

    def when_online(self, agent=None, actors=None, ts=None, **kw):
        self._update_agent(agent, actors, ts)

    def when_wakeup(self, **kw):
        self.presence.send_heartbeat()

    def when_heartbeat(self, agent=None, actors=None, ts=None, **kw):
        self._update_agent(agent, actors, ts)

    def when_offline(self, agent=None, actors=None, ts=None, **kw):
        self._remove_agent(agent)

    def expire_agents(self):
        expired = set()
        for id, state in self._agents.iteritems():
            if time() > state["ts"] + self.heartbeat_expire:
                expired.add(id)

        for id in expired:
            self._remove_agent(id)
        return self._agents

    def _update_agent(self, agent, actors, ts):
        self._agents[agent] = {"actors": actors, "ts": ts}

    def _remove_agent(self, agent):
        self._agents.pop(agent, None)

    @property
    def agents(self):
        return self.expire_agents()


class Presence(ConsumerMixin):
    State = State

    exchange = Exchange("cl.agents", type="topic", auto_delete=True)
    interval = 5
    _channel = None

    def __init__(self, agent, interval=None, use_eventlet=False, on_awake=None):
        self.agent = agent
        self.state = self.State(self)
        self.interval = interval or self.interval
        self.use_eventlet = use_eventlet
        self.connection = agent.connection
        self.on_awake = on_awake

    def get_queue(self):
        return Queue("cl.agents.%s" % (self.agent.id, ), self.exchange,
                     routing_key="#", auto_delete=True)

    def get_consumers(self, Consumer, channel):
        return [Consumer(self.get_queue(),
                         callbacks=[self.state.on_message], no_ack=True)]

    def Event(self, type):
        return {"agent": self.agent.id,
                "event": type,
                "actors": [actor.name for actor in self.agent.actors],
                "ts": time()}

    @contextmanager
    def extra_context(self, connection, channel):
        self.send_online()
        self.wakeup()
        sleep(1.0)
        if self.on_awake:
            self.on_awake()
        if self.use_eventlet:
            self._timer_eventlet(self.interval, self.send_heartbeat)
        yield
        self.send_offline()

    def announce(self, event):
        agent = self.agent
        routing_key = self.agent.id
        with producers[agent.connection].acquire(block=True) as producer:
            producer.publish(event, exchange=self.exchange.name,
                                    routing_key=routing_key)

    def start(self):
        if self.use_eventlet:
            self._spawn_eventlet(self.run)

    def send_online(self):
        return self.announce(self.Event("online"))

    def send_heartbeat(self):
        return self.announce(self.Event("heartbeat"))

    def send_offline(self):
        return self.announce(self.Event("offline"))

    def wakeup(self):
        return self.announce(self.Event("wakeup"))

    def _spawn_eventlet(self, fun, *args, **kwargs):
        import eventlet
        return eventlet.spawn(fun, *args, **kwargs)

    def _timer_eventlet(self, interval, fun, *args, **kwargs):
        from eventlet import greenthread

        def _re(interval):
            try:
                greenthread.spawn(fun, *args, **kwargs).wait()
            except Exception, exc:
                warnings.warn("Periodic timer %r raised: %r" % (fun, exc),
                              exc_info=sys.exc_info())
            finally:
                greenthread.spawn_after(interval, _re, interval=interval)
        return greenthread.spawn_after(interval, _re, interval=interval)

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

    @cached_property
    def presence(self):
        return Presence(self, use_eventlet=True, on_awake=self.on_awake)
