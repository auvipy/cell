from __future__ import absolute_import

from kombu.common import uuid

from cell.results import AsyncResult
from cell.actors import Actor

from .monads import mreturn, MonadReturn

__all__ = ['Workflow']


class Workflow(object):

    def __init__(self, protocol, id=None):
        self._wf_table = {}
        self.protocol = protocol
        self.id = id if id else self._build_conv_id()

    def __getitem__(self, to_role):
        print("In._get_from_conv_table")
        self._wf_table.setdefault(to_role, AsyncResult())
        if isinstance(self._wf_table[to_role], AsyncResult):
            # @TODO. Need timeout for the AsyncResult
            print("Wait on the Async Result")
            to_role_addr = self._wf_table[to_role].get()
            print("get the Async Result, value is:%s" % to_role_addr)
            self._wf_table[to_role] = to_role_addr
        return self._wf_table[to_role]

    def __setitem__(self, to_role, to_role_addr):
        print("Conv._add_to_conv_table: to_role:%s, to_role_addr:%s" % (
              to_role, to_role_addr))
        if to_role in self._conv_table and \
                isinstance(self._conv_table[to_role], AsyncResult):
            self._wf_table[to_role].set(to_role_addr)
        else:
            self._wf_table[to_role] = to_role_addr

    def has_role(self, role):
        return role in self._conv_table

    # TODO: Why we need the counter here?.
    # This is a copy from endpoint.py, it should be changed
    def _build_workflow_id(self):
        """
        Builds a unique conversation id.
        """
        return uuid()


class Server(Actor):
    """An actor which responds to the call protocol by looking for the
    specified method and calling it.

    Also, Server provides start and stop methods which can be overridden
    to customize setup.
    """

    def get_handler(self, message):
        if message.properties.get('reply_to'):
            handler = self.handle_call
        else:
            handler = self.handle_cast
        return handler()

    def start(self, *args, **kwargs):
        """Override to be notified when the server starts."""
        pass

    def stop(self, *args, **kwargs):
        """Override to be notified when the server stops."""
        pass

    def main(self, *args, **kwargs):
        """Implement the actor main loop by waiting forever for messages."""
        self.start(*args, **kwargs)
        try:
            while 1:
                body, message = yield self.receive()
                handler = self.get_handler(message)
                handler(body, message)
        finally:
            self.stop(*args, **kwargs)


class RPCClient(Actor):

    def __init__(self, server):
        self.server = server

    def request_internal(self, method, args):
        self.server.send({'method': method, 'args': args}, nowait=True)
        result = (yield self.server.receive())
        mreturn(result)

    def request(self, method, args):
        try:
            self.request_internal(method, args)
        except MonadReturn as val:
            return val
