"""cl.agents"""
import sys
import logging

from cl.common import uuid
from cl.consumers import ConsumerMixin


class Agent(ConsumerMixin):
    actors = []

    def __init__(self, connection, id=None, actors=None):
        self.connection = connection
        self.id = id or uuid()
        if actors is not None:
            self.actors = actors
        self.actors = self.prepare_actors()

    def run(self):
        self.info("Agent starting...")
        self.info("acts for %r" % ([actor.name for actor in self.actors], ))

    def run_from_commandline(self):
        logger = logging.getLogger()
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler(sys.stderr))
            logger.setLevel(logging.INFO)
        try:
            self.run()
        except KeyboardInterrupt:
            logger.info("[Quit requested by user]")

    def prepare_actors(self):
        return [actor.bind(self.connection) for actor in self.actors]

    def get_consumers(self, Consumer, channel):
        return [actor.Consumer(channel) for actor in self.actors]
