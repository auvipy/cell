from __future__ import absolute_import

from kombu import Connection, uuid

from cell import Actor
from cell.tests.utils import Case, Mock


class A(Actor):
    pass


class test_Actor(Case):

    def test_init(self):
        """test that __init__ sets fields"""

        a1 = A()
        self.assertIsNotNone(a1.exchange)
        self.assertIsNotNone(a1.outbox_exchange)
        self.assertTrue(a1.type_to_queue)
        self.assertTrue(a1.type_to_exchange)
        self.assertIsNotNone(a1.state)
        self.assertIsNotNone(a1.log)

    def test_init_construct(self):
        """test that __init__ calls construct callback"""

        class Constructed(Actor):
            construct_called = False

            def construct(self):
                self.construct_called = True

        self.assertTrue(Constructed().construct_called)

    def test_bind(self):
        """test when Actor.bind(connection)"""
        a = A()
        self.assertTrue(a.id)
        with Connection('memory://') as conn:
            bound = a.bind(conn)

            self.assertIsNot(a, bound, 'bind returns new instance')
            self.assertIs(bound.connection, conn)
            self.assertEqual(bound.id, a.id)
            self.assertEqual(bound.exchange, a.exchange)
            self.assertEqual(bound.name, a.name)
            self.assertIsNone(bound.agent)

    def test_bind_with_agent(self):
        """test when Actor.bind(connection, agent)"""
        a = A()
        agent = Mock(id=uuid())
        with Connection('memory://') as conn:
            bound = a.bind(conn, agent=agent)
            self.assertIs(bound.agent, agent)
            self.assertIs(bound.state.agent, agent)

    def test_contributes_to_state(self):
        """test that Actor.contributes_to_state"""


        class Stateful(Actor):

            class state(object):
                foo = 3

        class OverridesStateful(Actor):

            class state(object):

                def contribute_to_state(self, actor):
                    self.contributed = 1, actor
                    return self

        a1 = Stateful()
        self.assertIsNotNone(a1.state)
        self.assertIsInstance(a1.state, Stateful.state)
        self.assertEqual(a1.state.foo, 3)
        self.assertIs(a1.state.actor, a1)
        self.assertIsNone(a1.state.agent)
        self.assertIs(a1.state.connection, a1.connection)
        self.assertIs(a1.state.log, a1.log)
        self.assertIs(a1.state.Next, a1.Next)
        self.assertIs(a1.state.NoRouteError, a1.NoRouteError)
        self.assertIs(a1.state.NoReplyError, a1.NoReplyError)
        self.assertTrue(callable(a1.state.add_binding))
        self.assertTrue(a1.state.add_binding.__self__, a1)
        self.assertTrue(callable(a1.state.remove_binding))
        self.assertTrue(a1.state.remove_binding.__self__, a1)

        a2 = OverridesStateful()
        self.assertIsNotNone(a2.state)
        self.assertIsInstance(a2.state, OverridesStateful.state)
        self.assertTupleEqual(a2.state.contributed, (1, a2))
        with self.assertRaises(AttributeError):
            a2.state.actor
