class Infix:

    def __init__(self, function):
        self.function = function

    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))

    def __or__(self, other):
        return self.function(other)

    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))

    def __rshift__(self, other):
        return self.function(other)

    def __call__(self, value1, value2):
        return self.function(value1, value2)

# Examples


# simple multiplication
send = Infix(lambda channel, task: channel * task)
recv = Infix(lambda channel, task: channel * task)
to = Infix(
    lambda in_actor, out_actor: connect_actor_ports(in_actor, out_actor)
)


def connect_actor_ports(in_actor, out_actor):
    in_actor.connect_out(out_actor.in_ports['default'])
