import celery
from cell.actors import Actor
from cell.agents import dAgent
from cell.utils.custom_operators import Infix
import time
from celery.utils.imports import instantiate

my_app = celery.Celery(broker='pyamqp://guest@localhost//')

#celery.Celery().control.broadcast('shutdown')
#from examples.workflow import FilterExample
#from examples.workflow import actors_mng
#f = FilterExample()
#f.start()
#from examples.workflow import TestActor
#t = TestActor()

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

    class state(Actor.state):
        pass

    def become_remote(self, actor):
        return self.add_actor(actor)


class TrueFilter(WorkflowActor):

    class state(WorkflowActor.state):
        def filter(self,  msg):
            print 'Msg:%s received in filter' % (msg)
            self.actor.emit('notify', {'msg': msg})


class FalseFilter(WorkflowActor):

    class state(WorkflowActor.state):
        def filter(self,  msg):
            print 'Msg:%s received in filter' % (msg)
            self.actor.emit('notify', {'msg': msg})


class Joiner(WorkflowActor):
    def __init__(self, connection=None, *args, **kwargs):
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
            print 'In notify with count: %s and msg:%s' % (self.count,
                                                           msg)
            self.count += 1
            if self.count == len(self.sources):
                print 'I am sending the message to whoever is subscribed'
                self.actor.emit('set_ready', {'msg': 'ready'})
                self.count = 0


class GuardedActor(WorkflowActor):
    def __init__(self, connection=None, *args, **kwargs):
        self.ready = False
        super(GuardedActor, self).__init__(
            connection or my_app.broker_connection(), *args, **kwargs)

    class state(WorkflowActor.state):
        def set_ready(self,  msg):
            self.ready = True
            self.do_smth()

        def do_smth(self):
            print 'I have finally received all messages.'


class Printer(GuardedActor):
    types = ('scatter', 'round-robin', 'direct')

    class state(GuardedActor.state):
        def do_smth(self):
            print 'I am a printer'


class Logger(GuardedActor):

    def default_receive(self, msg):
        print msg

    class state(GuardedActor.state):
        pass


class Workflow(object):
    actors = []

    def __init__(self, actors):
        self.actors_mng = dAgent(
            connection=my_app.broker_connection(),
            app=my_app)
        self.actors = actors

    def start(self):
        for actor in self.actors:
            yield self.actors_mng.spawn(actor)


def join(outboxes, inbox):
    inbox.wait_to_start()
    inbox.call('set_sources',
               {'sources': [outbox.name for outbox in outboxes]},
               nowait=False)
    print 'send set_sources to collector'
    for outbox in outboxes:
        inbox.add_binding(outbox.outbox,
                          routing_key=outbox.routing_key,
                          inbox_type='direct')


def forward(source_actor, dest_actor):
    dest_actor.add_binding(source_actor.outbox,
                           routing_key=source_actor.routing_key,
                           inbox_type='scatter')


def stop_forward(source_actor, dest_actor):
    dest_actor.remove_binding(source_actor.outbox,
                              routing_key=source_actor.routing_key,
                              inbox_type='direct')


def multilplex(outbox, inboxes):
    for inbox in inboxes:
        inbox.wait_to_start()
        inbox.add_binding(outbox.outbox,
                          routing_key=outbox.routing_key,
                          inbox_type='direct')

join = Infix(join)

forward = Infix(forward)

multiplex = Infix(multilplex)

stop_forward = Infix(stop_forward)


def start_group(actor_type, count):
    actor_group = []
    [actor_group.append(instantiate(actor_type)) for _ in range(0, count)]
    wf = Workflow(actor_group)
    remote_group = list(wf.start())
    return remote_group


class FilterExample:
    def start(self):
        filter1, filter2, printer = TrueFilter(), FalseFilter(), Printer(),
        logger, collector = Logger(), Joiner()
        print 'collector_id before start:' + collector.id
        wf = Workflow([filter1, filter2, printer, logger, collector])
        [filter1, filter2, printer, logger, collector] = list(wf.start())
        print 'collector_id after start:' + collector.id

        time.sleep(2)

        [filter1, filter2] |join| collector
        collector |multiplex| [printer, logger]

        filter1.call('filter', {'msg': 'Ihu'})
        filter2.call('filter', {'msg': 'Ahu'})

printer_name = 'examples.workflow.Printer'
"""actors_mng = ActorsManager(connection = my_app.broker_connection(),
                              app = my_app)
"""

agent = dAgent(connection=my_app.broker_connection())

if __name__ == '__main__':
        printer = Printer()
        agent.spawn(printer)

"""Example usage:
>>from examples.workflow import Printer, Logger, actors_mng
Start 2 actors of type Printer remotely
>>rpr1 = agent.spawn(Printer)
>>rpr2 = agent.spawn(Printer)
Use remote actor
>>rpr.call('do_smth')
>>rpr.scatter('do_smth')
>>rpr.throw('do_smth')
Start another actor
>>rlog = agent.spawn(Log)
Bind two actors together
>>from examples.workflow import forward, stop_forward
>>rlog |forward| rpr1
Send to the output of ane actor and checks the binded actor receives is
>>rlog.emit('do_smth') # here do_smth of rpt1 should be invoked
Unbind actors
>>rlog |stop_forward| rpr1
Stop actors
>>agent.stop_actor_by_id(rlog.id)
>>agent.stop_actor_by_id(rpr1.id)
>>agent.stop_actor_by_id(rpr2.id)
"""
