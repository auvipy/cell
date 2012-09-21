from __future__ import absolute_import

from collections import deque

from .monads import callcc, done, ContinuationMonad, do


class Mailbox(object):

    def __init__(self, name=None):
        self.name = name
        self.messages = deque()
        self.handlers = deque()

    def send(self, message):
        try:
            handler = self.handlers.popleft()
        except IndexError:
            self.messages.append(message)
        else:
            handler(message)()

    def receive(self):
        return callcc(self.react)

    @do(ContinuationMonad)
    def react(self, handler):
        try:
            message = self.messages.popleft()
        except IndexError:
            self.handlers.append(handler)
            done(ContinuationMonad.zero())
        else:
            yield handler(message)
