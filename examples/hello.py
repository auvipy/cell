from kombu.common import uuid
from cell.actors import Actor, ActorProxy
from cell.agents import dAgent
from kombu.connection import Connection


class GreetingActor(Actor):
    class state(Actor.state):
        def greet(self, who='world'):
            return 'Hello %s' % who


class ByeActor(Actor):
    class state(GreetingActor.state):
        def bye(self, who='world'):
            print 'Bye %s' % who


# Run from the command line:
"""
from kombu import Connection
from examples.hello import GreetingActor
from cell.agents import dAgent


agent = dAgent(Connection())
greeter = agent.spawn(GreetingActor)
greeter.call('greet')
greeter = agent.select(GreetingActor)
from examples.workflow import Printer
id = agent.select(Printer)
"""


if __name__ == '__main__':
    """agent = dAgent(Connection())
    actor = agent.spawn(GreetingActor)

    actor.send.greet({'who':'hello'})
    actor.send.greet({'who':'hello'})
    first_reply(actor.scatter.greet({'who':'hello'}))
    """
