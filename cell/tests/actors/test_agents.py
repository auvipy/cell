from mock import patch, Mock, ANY
from cell.actors import Actor, ActorProxy, ACTOR_TYPE
from cell.agents import dAgent
from cell.tests.utils import Case, with_in_memory_connection
from cell.utils import qualname
from kombu.utils import uuid


class dA(dAgent):
    pass


class A(Actor):
    pass


class test_dAgent(Case):

    @patch('cell.actors.Actor')
    @patch('kombu.Connection')
    def test_init(self, conn, actor):
        id = uuid()
        a = dA(conn, id)
        self.assertEqual(a.connection, conn)
        self.assertEqual(a.id, id)
        self.assertEqual(a.agent, a)

    @patch('cell.actors.uuid', return_value=uuid())
    @patch('cell.agents.uuid', return_value=uuid())
    @with_in_memory_connection
    def test_add_actor(self, conn, actor_static_id, ticket_static_id):
        # Ensure the ActorProxy is returned
        # Ensure cast is invoked with the correct arguments

        ag, a = dA(conn), A()
        ag.cast = Mock()

        proxy = ag.add_actor(a)

        ag.cast.assert_called_once_with(
            'add_actor',
            {'name': qualname(a), 'id': actor_static_id.return_value},
            ANY, reply_to=ticket_static_id.return_value,
            type=ACTOR_TYPE.RR, nowait=False)

        self.assertIsInstance(proxy, ActorProxy)
        self.assertEqual(proxy.async_start_result.ticket,
                         ticket_static_id.return_value)

        # Agent state is not affected by the remote add_actor call
        self.assertDictEqual(ag.registry, {})

    @with_in_memory_connection
    @patch('cell.agents.uuid', return_value=uuid())
    def test_stop_actor_by_id(self, conn, static_id):
        ag = dA(conn)
        ag.cast = Mock()

        ag.stop_actor_by_id(static_id)

        ag.cast.assert_called_once_with(
            'stop_actor', {'actor_id': static_id},
            ANY, reply_to=ANY, type=ACTOR_TYPE.SCATTER,
            timeout=ag.default_timeout)

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_state_add_actor(self, conn, consumer):
        ag, a, id = dA(conn), A(), uuid()

        self.assertEquals(ag.registry, {})
        ag.state.add_actor(qualname(a), id)

        self.assertEquals(len(ag.registry), 1)
        actor = ag.registry[id]
        self.assertIs(type(actor), A)
        self.assertIsNotNone(actor.consumer)
        actor.consumer.consume.assert_called_once_with()

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_state_stop_actor_by_id(self, conn, consumer):
        ag, a, id = dA(conn), A(), uuid()
        ag.state.add_actor(qualname(a), id)
        self.assertEquals(len(ag.registry), 1)
        actor = ag.registry[id]

        ag.state.stop_actor(id)

        self.assertEquals(ag.registry, {})
        actor.consumer.cancel.assert_called_once_with()

    @with_in_memory_connection
    def test_state_stop_all(self, conn):
        ag, a = dA(conn), A()
        id1, id2 = uuid(), uuid()
        ag.state.add_actor(qualname(a), id1)
        ag.state.add_actor(qualname(a), id2)
        self.assertEquals(len(ag.registry), 2)
        actor1, actor2 = ag.registry[id1], ag.registry[id2]

        ag.state.stop_all()

        self.assertEquals(ag.registry, {})
        self.assertEquals(actor1.consumer.channel.queues, {})
        self.assertEquals(actor2.consumer.channel.queues, {})

    @with_in_memory_connection
    def test_stop(self, conn):
        ag, a, id1, id2 = dA(conn), A(), uuid(), uuid()
        ag.state.add_actor(qualname(a), id1)
        ag.state.add_actor(qualname(a), id2)
        self.assertEquals(len(ag.registry), 2)
        actor1, actor2 = ag.registry[id1], ag.registry[id2]

        ag.stop()

        self.assertEquals(len(ag.registry), 2)
        self.assertEquals(actor1.consumer.channel.queues, {})
        self.assertEquals(actor2.consumer.channel.queues, {})

    @with_in_memory_connection
    def test_start_when_actors_are_already_in_the_registry(self, conn):
        ag, a1, a2 = dA(conn), A(conn), A(conn)
        ag.registry.update({a1.id: a1, a2.id: a2})

        ag.start()

        self.assertIsNotNone(a1.consumer)
        self.assertIsNotNone(a2.consumer)
        self.assertEqual(len(ag.registry), 2)

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_reset(self, conn, consumer):
        ag, a1 = dA(conn), A()
        ag.state.add_actor(qualname(a1), a1.id)
        a1 = ag.registry[a1.id]

        ag.state.reset()

        self.assertIsNotNone(a1.consumer)
        self.assertEqual(len(ag.registry), 1)
        self.assertEqual(a1.consumer.cancel.call_count, 1)

    @with_in_memory_connection
    @patch('cell.agents.warn', return_value=Mock())
    def test_add_actor_when_id_in_registry(self, conn, warn):
        ag, a1 = dA(conn), A(conn)
        ag.registry[a1.id] = a1

        ag.state.add_actor(qualname(a1), a1.id)

        warn.assert_called_once_with(ANY, a1.id)

    @with_in_memory_connection
    @patch('cell.agents.warn', return_value=Mock())
    def test_stop_actor_when_id_not_in_registry(self, conn, warn):
        ag, a1 = dA(conn), A(conn)
        self.assertEqual(ag.registry, {})

        ag.state.stop_actor(a1.id)

        warn.assert_called_once_with(ANY, a1.id)
