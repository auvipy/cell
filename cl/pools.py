from itertools import chain

from kombu.pools import ProducerPool

_limit = [100]


class Connections(dict):

    def __missing__(self, connection):
        k = self[connection] = connection.Pool(_limit[0])
        return k
connections = Connections()


class Producers(dict):

    def __missing__(self, conn):
        k = self[conn] = ProducerPool(connections[conn], limit=_limit[0])
        return k
producers = Producers()


def _all_pools():
    return chain(connections.itervalues() if connections else iter([]),
                 producers.itervalues() if producers else iter([]))


def set_limit(limit):
    _limit[0] = limit
    for pool in _all_pools():
        pool.limit = limit
    return limit


def reset():
    global connections
    global producers
    for pool in _all_pools():
        try:
            pool.force_close_all()
        except Exception:
            pass
    connections = Connections()
    producers.Producers()


try:
    from multiprocessing.util import register_after_fork
    register_after_fork(connections, reset)
except ImportError:
    pass
