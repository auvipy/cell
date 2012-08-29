import cell
import celery
from cell import Actor
from kombu import Connection, Producer
from cell.utils.custom_operators import Infix 
from kombu.common import maybe_declare
my_app = celery.Celery(broker='pyamqp://guest@localhost//')
            
#celery.Celery().control.broadcast('shutdown')
#from examples.workflow import FilterExample
#f = FilterExample()
#f.start()

""" Simple scenario.
We have a Filter that filter collections and we want every result
to be send to Logger that do intensive computation on the filtered result
and to a Printer that do diverse visualizations.
The topology looks like that: 
Filter -> (Logger | Printer)
"""

class WorkflowActor(Actor):
    def __init__(self, connection=None, *args, **kwargs):
        super(WorkflowActor, self).__init__(
                connection or my_app.broker_connection(), *args, **kwargs)
        
    def start_remotely(self):
        name = "%s.%s"%(self.__class__.__module__, 
                        self.__class__.__name__)
        print name
        my_app.control.start_actor(name)


class TrueFilter(WorkflowActor):
    default_routing_key = 'filter'
        
    class state(WorkflowActor.state):
        def filter(self,  msg):
            print 'Msg:%s received in filter' %(msg)
            self.actor.emit('notify', {'msg': msg}, 
                            routing_key = '__scatter__')


class FalseFilter(WorkflowActor):
    default_routing_key = 'filter'
        
    class state(WorkflowActor.state):
        def filter(self,  msg):
            print 'Msg:%s received in filter' %(msg)
            self.actor.emit('notify', {'msg': msg}, 
                            routing_key = '__scatter__')


class Joiner(WorkflowActor):
    default_routing_key = 'collector'
    def __init__(self, connection=None, *args, **kwargs):
        self.sources = []
        super(Joiner, self).__init__(
                connection or my_app.broker_connection(), *args, **kwargs)
    
    def is_mutable(self):
        return False
    
    class state(WorkflowActor.state):
        def set_sources(self, sources):
            print 'In set_source, Collector. Count limit is', len(sources)
            self.count = 0
            self.sources = sources
        
        def notify(self,  msg):
            print 'In notify with count: %s and msg:%s' %(self.count, 
                                                          msg)
            self.count+=1
            if self.count == len(self.sources):
                self.actor.emit('set_ready', {'msg':'ready'}, 
                                routing_key = '__scatter__')
                self.count = 0

                      
class GuardedActor(WorkflowActor):
    default_routing_key = 'waiter'
    def __init__(self, connection=None, *args, **kwargs):
        self.ready = False
        super(GuardedActor, self).__init__(
                connection or my_app.broker_connection(), *args, **kwargs)
    
    class state():
        def set_ready(self,  msg):
            self.ready = True
            self.do_smth()
        
        def do_smth(self):
            print 'I have finally received all messages.'

        
class Printer(GuardedActor):
    default_routing_key = 'printer'
    class state(GuardedActor.state):
        def do_smth(self):
            print 'I am a printer'

            
class Logger(GuardedActor):
    default_routing_key = 'logger'
    def default_receive(self, msg):
        print msg
    class state(GuardedActor.state):
        pass


class Workflow(object):
    actors = []
    def __init__(self, actors):
        [actor.start_remotely() for actor in actors] 

        
def join(outboxes, inbox):
    inbox.send('set_sources', {'sources': [outbox.name for outbox in outboxes]})
    for outbox in outboxes:
        maybe_declare(outbox, outbox.channel)
        inbox.inbox_scatter.exchange_bind(outbox)     

def forward(outbox_exchange, inbox_exchange):
    maybe_declare(inbox_exchange, inbox_exchange.channel)
    maybe_declare(outbox_exchange, outbox_exchange.channel)
    inbox_exchange.exchange_bind(outbox_exchange)
    
def multilplex(outbox_exchange, inbox_exchanges):
    maybe_declare(outbox_exchange, outbox_exchange.channel)
    for inbox in inbox_exchanges:
        maybe_declare(inbox, inbox.channel)
        inbox.exchange_bind(outbox_exchange) 
        
join = Infix(join)

forward = Infix(forward)

multiplex = Infix(multilplex)

class FilterExample:
    def start(self):
        filter1, filter2, printer  = TrueFilter(), FalseFilter(), Printer(),
        logger, collector = Logger(), Joiner()   
        
        wf = Workflow([filter1, filter2, printer, logger, collector])
        
        [filter1.outbox, filter2.outbox] |join| collector
        collector.outbox |multiplex| [printer.inbox_scatter, logger.inbox_scatter]
        
        
        filter1.call('filter', {'msg':'Ihu'})
        filter2.call('filter', {'msg' :'Ahu'})

if __name__ == '__main__':        
        FilterExample().start()