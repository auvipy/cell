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


class Printer(Actor):
    default_routing_key = 'Printer'
           
    class state:
        def echo(self, msg = 'test'):
            print 'I am a printer:',msg
            #self.output_edge.send(msg)
            return msg 
        
printerActor = Printer(connection)



class Ihu(Actor):
    default_routing_key = 'Printer'
    
    class state:
        def temp(self, msg = 'blabla'):
            self.output_server.send(msg)
    

class GreetingAgent(Agent):
    actors = [greeting, printerActor]
    
if __name__ == '__main__':        
        consumer = GreetingAgent(connection).consume_from_commandline()
        for _ in consumer:
            print 'Received'

# Run this script from the command line and try this
# in another console:
#
#   >>> from hello import greeting
#   >>> greeting.call('greet')
#   'Hello world'
