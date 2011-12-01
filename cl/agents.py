"""cl.agents"""

from __future__ import absolute_import

from inspect import isclass

from kombu.common import uuid
from kombu.log import setup_logging
from kombu.mixins import ConsumerMixin

__all__ = ["Agent"]


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
        self.info("Agent on behalf of [%s] starting...",
                  ", ".join(actor.name for actor in self.actors))
        self.on_run()
        super(Agent, self).run()

    def stop(self):
        pass

    def on_consume_ready(self, *args, **kwargs):
        for actor in self.actors:
            actor.on_agent_ready()

    def run_from_commandline(self, loglevel=None, logfile=None):
        setup_logging(loglevel, logfile)
        try:
            self.run()
        except KeyboardInterrupt:
            self.info("[Quit requested by user]")

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
