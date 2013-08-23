Adder
=====

Actor that can add one to a given number.
Adder actor can be used to implement a Counter.

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

.. code-block:: python

    import celery
    from cell.actors import Actor
    from cell.agents import dAgent

    my_app = celery.Celery(broker='pyamqp://guest@localhost//')
    agent = dAgent(connection=my_app.broker_connection())


    class Aggregator(Actor):

        def __init__(self, barrier=None, **kwargs):
            self.barrier = barrier
            super(Aggregator, self).__init__(**kwargs)

        class state(Actor.state):
            def __init__(self):
                self.result = {}
                super(Aggregator.state, self).__init__()

            def aggregate(self, words):
                for word, n in words.iteritems():
                    self.result.setdefault(word, 0)
                    self.result[word] += n

                self.actor.barrier -= 1
                if self.actor.barrier <= 0:
                    self.print_result()

            def print_result(self):
                for (key, val) in self.result.iteritems():
                    print "%s:%s" % (key, val)


    class Reducer(Actor):

        class state(Actor.state):
            def __init__(self):
                self.aggregator = None
                super(Reducer.state, self).__init__()

            def on_agent_ready(self):
                self.aggregator = Aggregator(connection=self.actor.connection)

            def count_lines(self, line, aggregator):
                words = {}
                for word in line.split(" "):
                    words.setdefault(word, 0)
                    words[word] += 1
                self.aggregator.id = aggregator
                self.aggregator.call('aggregate', {'words': words})

        def on_agent_ready(self):
                self.state.on_agent_ready()


    class Mapper(Actor):

        class state(Actor.state):
            REDUCERS = 10

            def on_agent_ready(self):
                self.pool = []
                for i in range(self.REDUCERS):
                    reducer = self.actor.agent.spawn(Reducer)
                    self.pool.append(reducer)

            def count_document(self, file):
                with open(file) as f:
                    lines = f.readlines()
                    count = 0
                    self.aggregator = agent.spawn(Aggregator, barrier=len(lines))
                    for line in lines:
                        reducer = self.pool[count % self.REDUCERS]
                        reducer.cast('count_lines',
                                     {'line': line,
                                      'aggregator': self.aggregator.id})

        def on_agent_ready(self):
            self.state.on_agent_ready()

    if __name__ == '__main__':
        import examples.map_reduce
        file = "map_reduce_test.txt"
        mapper = agent.spawn(examples.map_reduce.Mapper)
        mapper.call('count_document', {'file': file})