from UserDict import DictMixin

from cl import Actor, Agent
from cl.utils import flatten


def first_reply(replies, key):
    try:
        return replies.next()
    except StopIteration:
        raise KeyError(key)


class Cache(Actor, DictMixin):
    types = ("scatter", "round-robin")
    default_timeout = 1

    class methods(object):

        def __init__(self, data=None):
            self.data = {}

        def get(self, key):
            if key not in self.data:
                # delegate to next agent.
                raise Actor.Next()
            return self.data[key]

        def delete(self, key):
            if key not in self.data:
                raise Actor.Next()
            return self.data.pop(key, None)

        def set(self, key, value):
            self.data[key] = value

        def keys(self):
            return self.data.keys()

    def __getitem__(self, key):
        return first_reply(self.scatter("get", {"key": key}), key)

    def __delitem__(self, key):
        return first_reply(self.scatter("delete", {"key": key}), key)

    def __setitem__(self, key, value):
        return self.throw("set", {"key": key, "value": value})

    def keys(self):
        return flatten(self.scatter("keys"))


class CacheAgent(Agent):
    actors = [Cache()]


if __name__ == "__main__":
    from kombu import Connection
    CacheAgent(Connection()).run_from_commandline()
