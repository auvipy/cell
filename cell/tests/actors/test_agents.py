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

    @with_in_memory_connection
    @patch('cell.agents.uuid', return_value=uuid())
    def test_kill_actor_by_id(self, conn, static_id):
        ag = dA(conn)
        ag.cast = Mock()

        ag.kill(static_id)

        ag.cast.assert_called_once_with(
            'kill', {'actor_id': static_id},
            reply_to=ANY, type=ACTOR_TYPE.SCATTER, declare=ANY,
            timeout=ag.default_timeout)

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_state_spawn_when_id_not_in_registry(self, conn, consumer):
        ag, a, id = dA(conn), A(), uuid()

        self.assertEquals(ag.state.registry, {})
        ag.state.spawn(qualname(a), id)

        self.assertEquals(len(ag.state.registry), 1)
        actor = ag.state.registry[id]
        self.assertIs(type(actor), A)
        self.assertIsNotNone(actor.consumer)
        actor.consumer.consume.assert_called_once_with()

    @with_in_memory_connection
    @patch('cell.agents.warn', return_value=Mock())
    def test_state_spawn_when_id_in_registry(self, conn, warn):
        ag, a1 = dA(conn), A(conn)
        ag.state.registry[a1.id] = a1

        ag.state.spawn(qualname(a1), a1.id)

        warn.assert_called_once_with(ANY, a1.id)

    @with_in_memory_connection
    @patch('cell.agents.error', return_value=Mock())
    def test_state_spawn_when_error_occurs(self, conn, error):
        ag, a1 = dA(conn), A(conn)
        ag.state.registry[a1.id] = a1
        ag.state._start_actor_consumer = Mock(
            side_effect=Exception('FooError'))

        ag.state.spawn(qualname(a1), a1.id)
        error.called_once_with('Cannot start actor: %r',
                               Exception('FooError'), ANY)

        ag.state._start_actor_consumer.reset_mock()
        ag.state._start_actor_consumer = Mock()
        error.reset_mock()
        ag.state.spawn('Ala Bala', a1.id)
        error.called_once_with('Cannot start actor: %r', 'ihu', ANY)

    @patch('cell.actors.ActorProxy', return_value=Mock())
    @patch('cell.actors.uuid', return_value=uuid())
    @patch('cell.agents.uuid', return_value=uuid())
    @with_in_memory_connection
    def test_spawn(self, conn, actor_static_id, ticket_static_id, proxy):
        # Ensure the ActorProxy is returned
        # Ensure cast is invoked with the correct arguments

        ag, a = dA(conn), A()
        ag.cast = Mock()

        proxy = ag.spawn(A)

        ag.cast.assert_called_once_with(
            'spawn',
            {'cls': qualname(a), 'id': actor_static_id.return_value,
             'kwargs': {}},
            reply_to=ticket_static_id.return_value, declare=ANY,
            type=ACTOR_TYPE.RR, nowait=False)

        # Check ActorProxy initialisation
        self.assertIsInstance(proxy, ActorProxy)
        self.assertEqual(proxy.id, actor_static_id.return_value)
        self.assertIsInstance(proxy._actor, A)
        self.assertEqual(proxy._actor.name, A().__class__.__name__)
        self.assertEqual(proxy._actor.connection, conn)
        self.assertEqual(proxy._actor.agent, ag)
        self.assertEqual(proxy.async_start_result.ticket,
                         ticket_static_id.return_value)

        # Agent state is not affected by the remote spawn call
        self.assertDictEqual(ag.state.registry, {})

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_state_stop_actor_by_id(self, conn, consumer):
        ag, a, id = dA(conn), A(), uuid()
        ag.state.spawn(qualname(a), id)
        self.assertEquals(len(ag.state.registry), 1)
        actor = ag.state.registry[id]

        ag.state.kill(id)

        self.assertEquals(ag.state.registry, {})
        actor.consumer.cancel.assert_called_once_with()

    @with_in_memory_connection
    def test_state_stop_all(self, conn):
        ag, a = dA(conn), A()
        id1, id2 = uuid(), uuid()
        ag.state.spawn(qualname(a), id1)
        ag.state.spawn(qualname(a), id2)
        self.assertEquals(len(ag.state.registry), 2)
        actor1, actor2 = ag.state.registry[id1], ag.state.registry[id2]

        ag.state.stop_all()

        self.assertEquals(ag.state.registry, {})
        self.assertEquals(actor1.consumer.channel.queues, {})
        self.assertEquals(actor2.consumer.channel.queues, {})

    @with_in_memory_connection
    def test_stop(self, conn):
        ag, a, id1, id2 = dA(conn), A(), uuid(), uuid()
        ag.state.spawn(qualname(a), id1)
        ag.state.spawn(qualname(a), id2)
        self.assertEquals(len(ag.state.registry), 2)
        actor1, actor2 = ag.state.registry[id1], ag.state.registry[id2]

        ag.stop()

        self.assertEquals(len(ag.state.registry), 2)
        self.assertEquals(actor1.consumer.channel.queues, {})
        self.assertEquals(actor2.consumer.channel.queues, {})

    @with_in_memory_connection
    def test_start_when_actors_are_already_in_the_registry(self, conn):
        ag, a1, a2 = dA(conn), A(conn), A(conn)
        ag.state.registry.update({a1.id: a1, a2.id: a2})

        ag.start()

        self.assertIsNotNone(a1.consumer)
        self.assertIsNotNone(a2.consumer)
        self.assertEqual(len(ag.state.registry), 2)

    @with_in_memory_connection
    @patch('cell.actors.Actor.Consumer', return_value=Mock())
    def test_reset(self, conn, consumer):
        ag, a1 = dA(conn), A()
        ag.state.spawn(qualname(a1), a1.id)
        a1 = ag.state.registry[a1.id]

        ag.state.reset()

        self.assertIsNotNone(a1.consumer)
        self.assertEqual(len(ag.state.registry), 1)
        self.assertEqual(a1.consumer.cancel.call_count, 1)

    @with_in_memory_connection
    def test_stop_actor_when_id_not_in_registry(self, conn):
        ag, a1 = dA(conn), A(conn)
        self.assertEqual(ag.state.registry, {})

        with self.assertRaises(Actor.Next):
            ag.state.kill(a1.id)

    @with_in_memory_connection
    def test_select_returns_scatter_results(self, conn):
        id1, id2 = uuid(), uuid()

        def scatter_result():
            yield id1
            yield id2

        ag = dAgent(conn)
        ag.scatter = Mock(return_value=scatter_result())

        proxy = ag.select(A)

        ag.scatter.assert_called_once_with(
            'select', {'cls': qualname(A)}, limit=1)

        # Check ActorProxy initialisation
        self.assertIsInstance(proxy, ActorProxy)
        self.assertEqual(proxy.id, id1)
        self.assertIsInstance(proxy._actor, A)
        self.assertEqual(proxy._actor.name, A().__class__.__name__)
        self.assertEqual(proxy._actor.connection, conn)
        self.assertEqual(proxy._actor.agent, ag)
        self.assertIsNone(proxy.async_start_result)

    @with_in_memory_connection
    def test_select_returns_error_when_no_result_found(self, conn):

        def scatter_result():
            yield None

        gen = scatter_result()
        gen.next()
        ag = dAgent(conn)
        ag.scatter = Mock(return_value=gen)

        with self.assertRaises(KeyError):
            ag.select(A)

    @with_in_memory_connection
    def test_state_select_returns_from_registry(self, conn):
        class B(Actor):
            pass

        ag, al = dAgent(conn), A(conn)
        id1, id2 = uuid(), uuid()

        with self.assertRaises(Actor.Next):
            ag.state.select(qualname(A))

        ag.state.registry[id1] = A()
        key = ag.state.select(qualname(A))

        self.assertEqual(key, id1)

        ag.state.registry[id2] = B(conn)
        keyA = ag.state.select(qualname(A))
        keyB = ag.state.select(qualname(B))
        self.assertEqual(keyA, id1)
        self.assertEqual(keyB, id2)

    @with_in_memory_connection
    def test_messages_processing_when_greenlets_are_enabled(self, conn):

        ag = dAgent(conn)
        ag.pool = Mock()
        ag.pool.is_green = True
        al, body, message = Mock(), Mock(), Mock()
        self.assertEqual(ag.is_green(), True)

        # message is processed in a separate pool if
        # greenlets are enabled and the sending actor is not an agent
        ag.process_message(al, body, message)
        ag.pool.spawn_n.assert_called_once_with(al._on_message, body, message)
        ag.pool.reset_mock()

        # message is always processed in a the same thread
        # if the sending actor is an agent
        ag._on_message = Mock()
        ag.process_message(ag, body, message)
        self.assertEqual(ag.pool.spawn_n.call_count, 0)
        ag._on_message.assert_called_once_with(body, message)

    @with_in_memory_connection
    def test_message_processing_when_greenlets_are_disabled(self, conn):
        ag = dAgent(conn)
        ag.pool = Mock()
        al = Mock()
        ag.pool.is_green = False
        body, message = Mock(), Mock()

        ag.process_message(al, body, message)

        al._on_message.assert_called_once_with(body, message)

        ag._on_message = Mock()
        ag.process_message(ag, body, message)
        self.assertEqual(ag.pool.spawn_n.call_count, 0)
        ag._on_message.assert_called_once_with(body, message)

    @with_in_memory_connection
    @patch('cell.agents.warn', return_value=Mock())
    def test_message_processing_warning(self, conn, warn):

        ag, al = dAgent(conn), A(conn)
        ag.pool = Mock()
        al._on_message = Mock()
        body, message = Mock(), Mock()

        # warning is not triggered when greenlets are disabled
        ag.pool.is_green = True
        ag.process_message(al, body, message)
        self.assertEquals(warn.call_count, 0)
        warn.reset_mock()

        # warning is not triggered when greenlets are enabled
        # but the call is not blocking
        ag.pool.is_green = True
        ag.process_message(al, body, message)
        self.assertEquals(warn.call_count, 0)
        warn.reset_mock()

        # warning is not triggered when greenlets are disables
        # and teh call is blocking
        ag.pool.is_green = False

        import cell
        cell.agents.itemgetter = Mock()
        message.properties = {'reply_to': '1234'}
        ag.process_message(al, body, message)
        warn.assert_called_once_with(
            'Starting a blocking call (%s) on actor (%s) when greenlets are disabled.',
            ANY, al.__class__)

        cell.agents.itemgetter.called_once_with('method')
