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
            for word, n in words.items():
                self.result.setdefault(word, 0)
                self.result[word] += n

            self.actor.barrier -= 1
            if self.actor.barrier <= 0:
                self.print_result()

        def print_result(self):
            for (key, val) in self.result.items():
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
    file = "/home/rumi/Dev/code/test/map_reduce.txt"
    mapper = agent.spawn(examples.map_reduce.Mapper)
    mapper.call('count_document', {'file': file})
