from cl import Actor, Agent

from kombu import Connection

connection = Connection()


class GreetingActor(Actor):
    default_routing_key = 'GreetingActor'

    class state:

        def greet(self, who='world'):
            return 'Hello %s' % who
greeting = GreetingActor(connection)


class GreetingAgent(Agent):
    actors = [greeting]


if __name__ == '__main__':
    GreetingAgent(connection).run_from_commandline()


# Run this script from the command line and try this
# in another console:
#
#   >>> from hello import greeting
#   >>> greeting.call('greet')
#   'Hello world'
