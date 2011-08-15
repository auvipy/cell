"""cl.agents"""

from __future__ import absolute_import

import sys
import logging

from .common import uuid
from .consumers import ConsumerMixin
from .log import setup_logger

__all__ = ["Agent"]


class Agent(ConsumerMixin):
    actors = []

    def __init__(self, connection, id=None, actors=None):
        self.connection = connection
        self.id = id or uuid()
        if actors is not None:
            self.actors = actors
        self.actors = self.prepare_actors()

    def run(self):
        self.info("Agent on behalf of [%s] starting..." % (
            ", ".join(actor.name for actor in self.actors), ))
        super(Agent, self).run()

    def run_from_commandline(self, loglevel=None, logfile=None):
        setup_logger(loglevel, logfile)
        try:
            self.run()
        except KeyboardInterrupt:
            self.info("[Quit requested by user]")

    def prepare_actors(self):
        return [actor.bind(self.connection) for actor in self.actors]

    def get_consumers(self, Consumer, channel):
        return [actor.Consumer(channel) for actor in self.actors]
