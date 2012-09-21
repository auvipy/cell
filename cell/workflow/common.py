from __future__ import absolute_import

from collections import deque

from .monads import callcc, done, ContinuationMonad, do


class Mailbox(object):

    def __init__(self, name=None):
        self.name = name
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
