"""cell.models"""

from __future__ import absolute_import

from kombu import Consumer, Queue
from komub.five import items, string_t
from kombu.utils import gen_unique_id

from . import Actor

_all__ = ['ModelActor', 'ModelConsumer']


class ModelConsumer(Consumer):
    model = None
    field = 'name'
    auto_delete = True

    def __init__(self, channel, exchange, *args, **kwargs):
        model = kwargs.pop('model', None)
        self.model = model if model is not None else self.model
        self.exchange = exchange
        self.prepare_signals(kwargs.pop('sigmap', None))
        queues = self.sync_queues(kwargs.pop('queues', []))
        super(ModelConsumer, self).__init__(channel, queues, *args, **kwargs)

    def prepare_signals(self, sigmap=None):
        for callback, connect in items(sigmap or {}):
            if isinstance(callback, string_t):
                callback = getattr(self, callback)
            connect(callback)

    def create_queue(self, field_value):
        return Queue(gen_unique_id(), self.exchange, field_value,
                     auto_delete=self.auto_delete)

    def sync_queues(self, keep_queues=[]):
        expected = [getattr(obj, self.field)
                    for obj in self.model._default_manager.enabled()]
        queues = set()
        create = self.create_queue

        for v in expected:
            queues.add(create(v))
        for queue in queues:
            if queue.routing_key not in expected:
                queues.discard(v)
        return list(keep_queues) + list(queues)

    def on_create(self, instance=None, **kwargs):
        fv = getattr(instance, self.field)
        if not self.find_queue_by_rkey(fv):
            self.add_queue(self.create_queue(fv))
            self.consume()

    def on_delete(self, instance=None, **kwargs):
        fv = getattr(instance, self.field)
        queue = self.find_queue_by_rkey(fv)
        if queue:
            self.cancel_by_queue(queue.name)

    def find_queue_by_rkey(self, rkey):
        for queue in self.queues:
            if queue.routing_key == rkey:
                return queue


class ModelActor(Actor):
    #: The model this actor is a controller for (*required*).
    model = None

    #: Map of signals to connect and corresponding actions.
    sigmap = {}

    def __init__(self, connection=None, id=None, name=None, *args, **kwargs):
        if self.model is None:
            raise NotImplementedError(
                "ModelActors must define the 'model' attribute!")
        if not name or self.name:
            name = self.model.__name__

        super(ModelActor, self).__init__(connection, id, name, *args, **kwargs)

    def Consumer(self, channel, **kwargs):
        return ModelConsumer(channel, self.exchange,
                             callbacks=[self.on_message],
                             sigmap=self.sigmap, model=self.model,
                             queues=[self.get_scatter_queue(),
                                     self.get_rr_queue()],
                             **kwargs)
