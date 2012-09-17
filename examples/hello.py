from cell import Actor, Agent
from cell.actors import Server

from kombu import Connection
from kombu.log import setup_logging 

connection = Connection()


class GreetingActor(Server):
    default_routing_key = 'GreetingActor'

    class state:
        def greet(self, who='world'):
            return 'Hello %s' % who
greeting = GreetingActor(connection)
 


class GreetingAgent(Agent):
    actors = [greeting]
    
if __name__ == '__main__':        
        GreetingAgent(connection).consume_from_commandline()
# Run this script from the command line and try this
# in another console:
#
#   >>> from hello import greeting
#   >>> greeting.call('greet')
#   'Hello world'
