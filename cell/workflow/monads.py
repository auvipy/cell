import types

###### Base Monad and @do syntax#########

class Monad:
    def bind(self, func):
        raise NotImplementedError

    def __rshift__(self, bindee):
        return self.bind(bindee)

    def __add__(self, bindee_without_arg):
        return self.bind(lambda _ : bindee_without_arg())

def make_decorator(func, *dec_args):
    def decorator(undecorated):
        def decorated(*args, **kargs):
            return func(undecorated, args, kargs, *dec_args) 
        
        decorated.__name__ = undecorated.__name__
        return decorated
    
    decorator.__name__ = func.__name__
    return decorator

def make_decorator_with_args(func):
    def decorator_with_args(*dec_args):
        return make_decorator(func, *dec_args)
    return decorator_with_args

decorator           = make_decorator
decorator_with_args = make_decorator_with_args

@decorator_with_args
def do(func, func_args, func_kargs, Monad):
    @handle_monadic_throws(Monad)
    def run_maybe_iterator():
        itr = func(*func_args, **func_kargs)

        if isinstance(itr, types.GeneratorType):
            @handle_monadic_throws(Monad)
            def send(val):
                try:
                    # here's the real magic
                    monad = itr.send(val) 
                    return monad.bind(send)
                except StopIteration:
                    return Monad.unit(None)
                
            return send(None)
        else:
            #not really a generator
            if itr is None:
                return Monad.unit(None)
            else:
                return itr

    return run_maybe_iterator()

@decorator_with_args
def handle_monadic_throws(func, func_args, func_kargs, Monad):
    try:
        return func(*func_args, **func_kargs)
    except MonadReturn, ret:
        return Monad.unit(ret.value)
    except Done, done:
        assert isinstance(done.monad, Monad)
        return done.monad

class MonadReturn(Exception):
    def __init__(self, value):
        self.value = value
        Exception.__init__(self, value)         

class Done(Exception):
    def __init__(self, monad):
        self.monad = monad
        Exception.__init__(self, monad)

def mreturn(val):
    raise MonadReturn(val)

def done(val):
    raise Done(val)

def fid(val):
    return val

##### Failable Monad ######

class Failable(Monad):
    def __init__(self, value, success):
        self.value   = value
        self.success = success

    def __repr__(self):
        if self.success:
            return "Success(%r)" % (self.value,)
        else:
            return "Failure(%r)" % (self.value,)    

    def bind(self, bindee):
        if self.success:
            return bindee(self.value)
        else:
            return self

    @classmethod
    def unit(cls, val):
        return cls(val, True)

class Success(Failable):
    def __init__(self, value):
        Failable.__init__(self, value, True)

class Failure(Failable):
    def __init__(self, value):
        Failable.__init__(self, value, False)

def failable_monad_examle():
    def fdiv(a, b):
        if b == 0:
            return Failure("cannot divide by zero")
        else:
            return Success(a / b)

    @do(Failable)
    def with_failable(first_divisor):
        val1 = yield fdiv(2.0, first_divisor)
        val2 = yield fdiv(3.0, 1.0)
        val3 = yield fdiv(val1, val2)
        mreturn(val3)

    print with_failable(0.0)
    print with_failable(1.0)

###### StateChanger Monad #########

class StateChanger(Monad):
    def __init__(self, run):
        self.run = run

    def bind(self, bindee):
        run0 = self.run

        def run1(state0):
            (result, state1) = run0(state0)
            return bindee(result).run(state1)

        return StateChanger(run1)

    @classmethod
    def unit(cls, val):
        return cls(lambda state : (val, state))

def get_state(view = fid):
    return change_state(fid, view)

def change_state(changer, view = fid):
    def make_new_state(old_state):
        new_state    = changer(old_state)
        viewed_state = view(old_state)
        return (viewed_state, new_state)
    return StateChanger(make_new_state)


def state_changer_monad_example():
    @do(StateChanger)
    def dict_state_copy(key1, key2):
        val = yield dict_state_get(key1)
        yield dict_state_set(key2, val)
        mreturn(val)

    @do(StateChanger)
    def dict_state_get(key, default = None):
        dct = yield get_state()
        val = dct.get(key, default)
        mreturn(val)

    @do(StateChanger)
    def dict_state_set(key, val):
        def dict_set(dct, key, val):
            dct[key] = val
            return dct

        new_state = yield change_state(lambda dct: dict_set(dct, key, val))
        mreturn(val)

    @do(StateChanger)
    def with_dict_state():
        val2 = yield dict_state_set("a", 2)
        yield dict_state_copy("a", "b")
        state = yield get_state()
        mreturn(val2)

    print with_dict_state().run({}) # (2, {"a" : 2, "b" : 2})

###### Continuation Monad #########
class ContinuationMonad(Monad):
    def __init__(self, run):
        self.run = run

    def __call__(self, cont = fid):
        return self.run(cont)        

    def bind(self, bindee):
        return ContinuationMonad(lambda cont : self.run(lambda val : bindee(val).run(cont)))

    @classmethod
    def unit(cls, val):
        return cls(lambda cont : cont(val))

    @classmethod
    def zero(cls):
        return cls(lambda cont : None)
    
def callcc(usecc):
    return ContinuationMonad(lambda cont : usecc(lambda val : ContinuationMonad(lambda _ : cont(val))).run(cont))

from collections import deque

class AgentRole:
    def receive(self, sender):
        yield self.receiver.receive()
        
    def send(self, recepient, message):
        recepient.send(message)
    
    def add_role(self, mailbox):
        self.roles.append(mailbox)        
    
class Mailbox:
    def __init__(self):
        self.messages = deque()
        self.handlers = deque()

    def send(self, message):
        if self.handlers:
            handler = self.handlers.popleft()
            handler(message)()
        else:
            self.messages.append(message)

    def receive(self):
        return callcc(self.react)

    @do(ContinuationMonad)
    def react(self, handler):
        if self.messages:
            message = self.messages.popleft()
            yield handler(message)
        else:
            self.handlers.append(handler)
            done(ContinuationMonad.zero())


from gevent import queue as gqueue 
class RemoteMailbox(Mailbox):
    # data for this receive channel
    _recv_queue     = None
    _consumer_tag   = None
    _recv_name      = None      # name this receiving channel is receiving on - tuple (exchange, queue)
    _recv_binding   = None      # binding this queue is listening on (set via _bind)
    
    
    def __init__(self, name, binding):
        self._recv_queue = gqueue.Queue()
        self._recv_name = name
        self._recv_binding = binding
        Mailbox.__init__()
    
    def send(self, message):
        if self.handlers:
            handler = self.handlers.popleft()
            handler(message)()
        else:
            self.messages.append(message)
    
    def receive(self):
        return callcc(self.react)
        
    @do(ContinuationMonad)
    def react(self, handler):
        if self.messages:
            message = self.messages.popleft()
            yield handler(message)
        else:
            self.handlers.append(handler)
            done(ContinuationMonad.zero())

        
def continuation_monad_example():

    @do(ContinuationMonad)
    def insert(mb, values):
        for val in values:
            mb.send(val)

    #This is a multiplier flowlet
    @do(ContinuationMonad)
    def multiply(mbin, mbout, factor):
        while True:
            val = (yield mbin.receive())
            mbout.send(val * factor)

    #This is a printer flowlet
    @do(ContinuationMonad)
    def print_all(mb):
        while True:
            print (yield mb.receive())
    
    #This is a flowlet for filtering        
    @do(ContinuationMonad)
    def filter(mbin, mbout, cond):
        print 'Entering filter'
        while True:
            val = (yield mbin.receive())
            print 'In filter:', val
            res = cond(val) 
            mbout.send((val, res))
    
    @do(ContinuationMonad)
    def recv(mbin, collector = None, is_rec = False):
        val = (yield mbin.receive())
        if collector: collector.send(val)
        if val: print val
        while is_rec:
            val = (yield mbin.receive())
            if collector: collector.send(val)
            if val: print val        
    
    @do(ContinuationMonad)
    def join(l, waiter):
        collector = Mailbox()
        par(l, collector)
        c = 0
        for count in l:
            val = (yield collector.receive())
            print 'Inside join', val
            c = c + 1
        print 'Counter is:', c
        waiter.send('Done')
                
    def csend(actor, task):
        actor.send(task)
    
    @do(ContinuationMonad)
    def creceive(actor):
        val = (yield actor.receive())
        mreturn (val)
    
    @do(ContinuationMonad)
    def execute(sink1, sink2, waiter):
        join([sink1, sink2], waiter)()
        add_callback(waiter)()
        print 'After join'
        sink1.send(1)
        sink2.send(2)
        print 'After sink send'
           
    def coroutine(func):
        def start(*args,**kwargs):
            cr = func(*args,**kwargs)
            cr.next()
            return cr
        return start
        
    def par(l, collector = None):
        for mbin in l:
            recv(mbin, collector, True)()
            
    
    def choice(cond, tasks, mbin):
        for task in tasks :
            if cond(task):
                mbin.send(task) 
                break
    def rec():
        pass
    
    @do(ContinuationMonad)
    def add_callback(waiter):
        print 'In do computation'
        s = (yield waiter.receive())
        print 'I think it is time to end this wonderful jurney:',s

    # worker@allice@workflow
    # create a conversation channel 
    # workflow is a combination of work(flow)lets
    # It specifies whether the actors are local or remote, provide a runtime guarantee if somethink is not right 
    # from the pool of workers, assign a handler to one of it
    # if you have receive, it's you
    original   = Mailbox() # channels
    multiplied = Mailbox() # channels, whenever it is send
    multiplied1 = Mailbox()
    sink = Mailbox()
    sink1 = Mailbox()
    sink2 = Mailbox()
    collector = Mailbox()
    waiter = Mailbox()
    val = creceive(sink1)
    print val
    # spawn an Actor on their behalf ... start doing it, cannot join here 
    #stimer = wf.start()
    execute(sink1, sink2, original)
    insert(original, [1, 2, 3])()
    multiply(original, multiplied, 3)()
    multiply(original, multiplied1, 4)() 
    filter(multiplied1, sink, lambda x: x%2 ==0)()
    #Note that here we wait on a collector, what does it means, that we want the collector to be triggered
    #par([multiplied, sink1], collector) 
    join([sink1, sink2], waiter)()
    add_callback(waiter)()
    print 'After join'
    sink1.send(1)
    sink2.send(2)
    print 'After sink send'
    print_all(sink)()    
        
    # So every workflow always has a collcetor, so the continuation only adds 
    
    @do(ContinuationMonad)
    #tests for actor framework
    def multiply_print(values, factor, orig, mult, me):
        for val in values:
            #send values to the originator
            me.to(orig).send(val)
            #originator receive the values, process them and send them to the 
            #multiplier
            result = val*factor
            orig.to(mult).send(result)
            #then the multiplier returns them to me
            mult.to(me).send(result)
            #Note we have data paralellism here,, not task parallelism
            
            #inserter [1, 2, 3]| multiplier | printer
            
if __name__ == "__main__":
    #failable_monad_examle()
    #state_changer_monad_example()
    #continuation_monad_example()
    pass