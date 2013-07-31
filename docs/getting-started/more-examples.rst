Adder
=====

.. code-block:: python

    from kombu import Connection
    connection = Connection()
    agent = dAgent(connection)

    class Adder(Actor):
        class state():
            def add_one(self, i):
                print 'Increasing %s with 1' % i
                return i + 1

    if __name__=='__main__':
        import examples.adder
        adder = agent.spawn(Adder)

        adder.call('add-one', {'i':10})

Chat-users
==========

.. code-block:: python

    from cell.actors import Actor
    from cell.agents import dAgent

    connection = Connection()

    class User(Actor):
        class state():

            def post(self, msg):
                print msg

        def post(self, msg):
            msg = 'Posting on the wall: %s' % msg
            self.scatter('post', {'msg': msg})

        def message_to(self, actor, msg):
            a = User(id = actor, connection = self.connection)
            msg = 'Actor %s is sending you a message: %s' %(self.id, msg)
            a.call('post', {'msg':msg})

        def connect(self):
            if not agent:
                agent = dAgent(self.connection)
            return self.agent.spawn(self)

    if __name__=='__main__':
        import examples.chat
        rumi = examples.chat.User(connection).connect()
        rumi.post('Hello everyone')

        ask = examples.chat.User(connection).connect()
        ask.post('Hello everyone')
        rumi.message_to(ask.id, 'How are you?')
        ask.message_to(rumi.id, 'Fine.You?')

Map-reduce
==========

