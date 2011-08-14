import operator

from itertools import imap, ifilter

def force_list(obj):
    if not hasattr(obj, "__iter__"):
        return [obj]
    return obj

def flatten(it):
    if it:
        return reduce(operator.add,
                imap(force_list, ifilter(None, it)))
    return it
