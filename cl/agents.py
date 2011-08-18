"""cl.agents"""

from __future__ import absolute_import, with_statement

import sys
import logging

from contextlib import contextmanager

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

    def on_run(self):
        pass

    def run(self):
        self.info("Agent on behalf of [%s] starting..." % (
            ", ".join(actor.name for actor in self.actors), ))
        self.on_run()
        super(Agent, self).run()

    def run_from_commandline(self, loglevel=None, logfile=None):
        setup_logger(loglevel, logfile)
        try:
            self.run()
        except KeyboardInterrupt:
            self.info("[Quit requested by user]")

    def prepare_actors(self):
        return [actor.bind(self.connection, self) for actor in self.actors]

    def get_consumers(self, Consumer, channel):
        return [actor.Consumer(channel) for actor in self.actors]

    def get_default_scatter_limit(self, actor):
        return None

