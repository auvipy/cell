import celery
from cell.actors import Actor
from cell.agents import dAgent
from cell.results import AsyncResult
from kombu.utils import uuid
from examples.workflow import forward

"""
actors_mng = dAgent(connection=my_app.broker_connection())
    Simple Adder app
Steps:
1. Start celery worker
2.
"""


my_app = celery.Celery(broker='pyamqp://guest@localhost//')


class Adder(Actor):
    def __init__(self, connection=None, *args, **kwargs):
        super(Adder, self).__init__(
            connection or my_app.broker_connection(), *args, **kwargs)

    class state():
        def add_one(self, i, token=None):
            print 'Increasing %s with one' % i
            res = i + 1
            self.actor.emit('count', {'res': res, 'token': token})
            return res


class Bookkeeper(Actor):
    def __init__(self, connection=None, *args, **kwargs):
        super(Bookkeeper, self).__init__(
            connection or my_app.broker_connection(), *args, **kwargs)

    class state():
        def __init__(self):
            self.targets = {}
            self.adder = None

        def on_agent_ready(self):
            ra = Adder(self.actor.connection)
            self.adder = self.actor.agent.add_actor(ra)
            self.adder |forward| self.actor

        def count(self, res, token):
            (target, cur) = self.targets.get(token)
            if cur < res < target:
                self.adder.call('add_one', {'i': res, 'token': token},
                                nowait=True)
            elif res >= target:
                self.targets.pop(token)

        def count_to(self, target):
            token = uuid()
            init = 0
            self.targets[token] = (target, init)
            self.adder.throw('add_one', {'i': init, 'token': token},
                             nowait=True)

    def on_agent_ready(self):
        self.state.on_agent_ready()

actors_mng = dAgent(connection=my_app.broker_connection())


class gBookkeeper(Actor):
    def __init__(self, connection=None, *args, **kwargs):
        super(gBookkeeper, self).__init__(
            connection or my_app.broker_connection(), *args, **kwargs)

    class state():
        def __init__(self):
            self.targets = {}
            self.adder = None

        def on_agent_ready(self):
            adder = Adder(self.actor.connection)
            self.adder = self.actor.agent.add_actor(adder)

        def count_to(self, target):
            i = 0
            while i <= target:
                i = self.adder.send('add_one', {'i': i, 'token': 0})
            return i

    def on_agent_ready(self):
        self.state.on_agent_ready()

if __name__ == '__main__':
    import examples.adder
    b = examples.adder.Bookkeeper()
    rb = actors_mng.add_actor(b)
    rb.call('configure_adder')
    rb.call('count_to', {'target': 10})
