"""cl.pools"""

from __future__ import absolute_import

from itertools import chain

from kombu.pools import ProducerPool

__all__ = ["connections", "producers", "set_limit", "reset"]
_limit = [100]


class _Connections(dict):

    def __missing__(self, connection):
        k = self[connection] = connection.Pool(_limit[0])
        return k
connections = _Connections()


class _Producers(dict):

    def __missing__(self, conn):
        k = self[conn] = ProducerPool(connections[conn], limit=_limit[0])
        return k
producers = _Producers()


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
    connections = _Connections()
    producers._Producers()


try:
    from multiprocessing.util import register_after_fork
    register_after_fork(connections, reset)
except ImportError:
    pass
