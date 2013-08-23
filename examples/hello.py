from cell.actors import Actor


class GreetingActor(Actor):

    class state:
        def greet(self, who='world'):
            print 'Hello %s' % who

#Run from the command line:
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
