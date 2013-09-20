from math import sqrt
from cell.actors import Actor
from cell.agents import dAgent
from cell.results import AsyncResult
from kombu.connection import Connection

class Abs(Actor):
    class state(object):

        def calc(self, val):
            if isinstance(val, AsyncResult):
                val = val.get()
            new_val = abs(val)
            print 'The abs result is:', new_val
            return new_val

class Square(Actor):
    class state(object):

        def calc(self, val):
            if isinstance(val, AsyncResult):
                val = val.get()
            new_val = val*val
            print 'The square result is:', new_val
            return new_val

class SquareRoot(Actor):
    class state(object):

        def calc(self, val):
            if isinstance(val, AsyncResult):
                val = val.get()
            new_val = sqrt(val)
            print 'The sqrt result is:', new_val
            return new_val

class Printer(Actor):
    def __init__(self, **kwargs):
        super(Printer, self).__init__(**kwargs)

    class state(object):
        def calc(self, val):
            if isinstance(val, AsyncResult):
                print 'receiving AsyncResult'
                val = val.get()
            print 'The printer result is:', val
            return val

import time
class Calculator(object):
    def __init__(self):
        self.agent = agent = dAgent(Connection())
        self.abs = agent.spawn(Abs)
        self.sqrt = agent.spawn(SquareRoot)
        self.square = agent.spawn(Square)
        self.printer = agent.spawn(Printer)

    def run(self, val):
        start = time.time()
        val = self.abs.send.calc({'val' :val})
        val = self.sqrt.send.calc({'val' :val})
        self.printer.call.send({'val' :val})
        total = start - time.time()
        print 'Finish in:', total



if __name__ == '__main__':
    import sys
    print sys.path
    calc = Calculator()
    calc.run(10)