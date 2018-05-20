import celery
from cell.actors import Actor
from cell.agents import dAgent

my_app = celery.Celery(broker='pyamqp://guest@localhost//')
agent = dAgent(connection=my_app.broker_connection())


class User(Actor):
    def __init__(self, connection=None, *args, **kwargs):
        super(User, self).__init__(
            connection or my_app.broker_connection(), *args, **kwargs)

    class state():

        def post(self, msg):
            print msg

        def connect(self):
            return agent.spawn(self.__class__)

    def connect(self, nickname):
        self.call('connect', {'name': nickname})

    def post(self, msg):
        msg = 'Posting on the wall: %s' % msg
        self.scatter('post', {'msg': msg})

    def message_to(self, actor, msg):
        a = User(id=actor, connection=self.connection)
        msg = 'Actor %s is sending you a message: %s' % (self.id, msg)
        a.call('post', {'msg': msg})


if __name__ == '__main__':
    import examples.chat
    rumi = examples.chat.User().connect()
    rumi.post('Hello everyone')

    ask = examples.chat.User().connect()
    ask.post('Hello everyone')
    rumi.message_to(ask.id, 'How are you?')
    ask.message_to(rumi.id, 'Fine.You?')
