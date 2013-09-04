from __future__ import absolute_import
import ast

from kombu import Connection
from kombu.common import uuid, maybe_declare
from mock import patch, ANY
from cell.actors import Actor, ActorProxy
from cell.exceptions import WrongNumberOfArguments
from cell.results import AsyncResult
from cell.tests.utils import Case, Mock, with_in_memory_connection
from cell.actors import ACTOR_TYPE
from kombu.compression import compress
from kombu.entity import Exchange
from kombu.exceptions import StdChannelError
from kombu.messaging import Consumer
from cell.utils import qualname


class A(Actor):
    pass


class RRActor(Actor):
    type = (ACTOR_TYPE.RR, )


class ScatterActor(Actor):
    type = (ACTOR_TYPE.RR, )


class MyCustomException(Exception):
    pass


def clean_up_consumers(consumers):
    for c in consumers:
        for q in c.queues:
            q.purge()


def get_next_msg(consumer):
        for q in consumer.queues:
            next_msg = q.get()
            if next_msg:
                consumer.channel.basic_ack(next_msg.delivery_tag)
                return next_msg


def get_test_message(method='foo', args={'bar': 'foo_arg'},
                     class_name=None, reply_to=None, delivery_tag=None):
    with Connection('memory://') as conn:
        ch = conn.channel()

        body = {'method': method, 'args': args, 'class': class_name}
        data = ch.prepare_message(body)
        data['properties']['reply_to'] = reply_to
        delivery_tag = delivery_tag or uuid()
        data['properties']['delivery_tag'] = delivery_tag

        return body, ch.message_to_python(data)


def get_encoded_test_message(method='foo', args={'bar': 'foo_arg'},
                             class_name=A.__class__.__name__,
                             reply_to=None, delivery_tag=None):
    with Connection('memory://') as conn:
        ch = conn.channel()

        body = {'method': method, 'args': args, 'class': class_name}
        c_body, compression = compress(str(body), 'gzip')
        data = ch.prepare_message(c_body, content_type='application/json',
                                  content_encoding='utf-8',
                                  headers={'compression': compression})
        data['properties']['reply_to'] = reply_to
        delivery_tag = delivery_tag or uuid()
        data['properties']['delivery_tag'] = delivery_tag
        return body, ch.message_to_python(data)


class test_Actor(Case):

    def assertNextMsgDataEqual(self, consumer, expected_data):
        next_msg = get_next_msg(consumer)
        msg = next_msg.decode()
        self.assertDictContainsSubset(expected_data, msg)

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
#--------------------------------------------------------------------
# Test all API send-like methods call cast with the correct arguments
#---------------------------------------------------------------------

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
#-----------------------------------------------------------------
# Test the API for invoking a remote method
#-----------------------------------------------------------------

    def test_throw(self):
        # Set Up
        method, args, return_val = 'foo', {'args': 'foo_args'}, 'result'
        a = A()
        a.call_or_cast = Mock(return_value=Mock())
        a.call_or_cast.return_value.get = Mock(return_value=return_val)

        # when throw is invoked,
        # all its arguments are passed to call_ot_cast and result is returned
        result = a.throw(method, args)
        a.call_or_cast.assert_called_once_with(method, args,
                                               type=ACTOR_TYPE.RR,
                                               nowait=False)
        self.assertEquals(result, return_val)
        a.call_or_cast.reset_mock()

        # when throw is invoked with no_wait=True, no result is returned
        result = a.throw(method, args, nowait=True)
        a.call_or_cast.assert_called_once_with(method, args,
                                               type=ACTOR_TYPE.RR,
                                               nowait=True)
        self.assertIsNone(result)
        a.call_or_cast.reset_mock()

        # when throw is invoked without arguments
        # empty list is passed to call_or_cast
        a.throw(method)
        a.call_or_cast.assert_called_once_with(method, {},
                                               type=ACTOR_TYPE.RR,
                                               nowait=False)

    def test_send(self):
        # Set Up
        method, args, return_val = 'foo', {'args': 'foo_args'}, 'bar'
        a = A()
        a.call_or_cast = Mock(return_value=Mock())
        a.call_or_cast.return_value.get = Mock(return_value=return_val)

        # when send is invoke all its arguments are passed to call_and_cast
        result = a.send(method, args)
        a.call_or_cast.assert_called_once_with(method, args,
                                               nowait=False,
                                               routing_key=a.id)
        self.assertIs(result, return_val)
        a.call_or_cast.reset_mock()

        # when send is invoke with nowait=True, result is returned
        result = a.send(method, args, nowait=False)
        a.call_or_cast.assert_called_with(method, args,
                                          nowait=False,
                                          routing_key=a.id)
        self.assertIs(result, return_val)
        a.call_or_cast.reset_mock()

        # when send is invoke with nowait=True, no result is returned
        result = a.send(method, args, nowait=True)
        a.call_or_cast.assert_called_once_with(method, args,
                                               nowait=True,
                                               routing_key=a.id)
        self.assertIsNone(result)
        a.call_or_cast.reset_mock()

        # when send is invoke without arguments
        # empty list is passed to call_or_cast
        result = a.send(method, nowait=True)
        a.call_or_cast.assert_called_with(method, {},
                                          nowait=True,
                                          routing_key=a.id)
        self.assertIsNone(result)

    def test_scatter(self):
        # Set Up
        method, args,  = 'foo', {'args': 'foo_args'}
        return_val, timeout, default_timeout = 'res', 1, 2

        a = A()
        a.default_timeout = default_timeout
        a.call_or_cast = Mock(return_value=Mock())
        a.call_or_cast.return_value.gather = Mock(return_value=return_val)

        # when scatter is invoked with default arguments
        result = a.scatter(method, args)
        a.call_or_cast.assert_called_once_with(method, args,
                                               type=ACTOR_TYPE.SCATTER,
                                               nowait=False,
                                               timeout=default_timeout)

        self.assertEquals(result, return_val)
        a.call_or_cast.reset_mock()

        # when scatter is invoked with explicit nowait and timeout
        nowait = False
        result = a.scatter(method, args, nowait, **{'timeout': timeout})
        a.call_or_cast.assert_called_once_with(method, args,
                                               type=ACTOR_TYPE.SCATTER,
                                               nowait=nowait,
                                               timeout=timeout)
        self.assertEquals(result, return_val)
        a.call_or_cast.reset_mock()

        # when scatter is invoked with explicit nowait set to True
        nowait = True
        result = a.scatter(method, args, nowait)
        a.call_or_cast.assert_called_once_with(method, args,
                                               type=ACTOR_TYPE.SCATTER,
                                               nowait=nowait,
                                               timeout=default_timeout)
        self.assertIsNone(result, None)
        a.call_or_cast.reset_mock()

        # when scatter is invoked without args param set
        result = a.scatter(method)
        a.call_or_cast.assert_called_once_with(method, {},
                                               type=ACTOR_TYPE.SCATTER,
                                               nowait=False,
                                               timeout=default_timeout)

    def test_emit(self):
        method, args, retry = 'foo', {'args': 'foo_args'}, True
        a = A()
        a.cast = Mock()

        # when emit is invoked with default arguments
        a.emit(method)
        result = a.cast.assert_called_once_with(method, {},
                                                retry=None,
                                                exchange=a.outbox)
        self.assertIsNone(result)
        a.cast.reset_mock()

        # when emit is invoked with explicit arguments
        a.emit(method, args, retry)
        result = a.cast.assert_called_once_with(method, args,
                                                retry=retry,
                                                exchange=a.outbox)
        self.assertIsNone(result)

    def test_call_or_cast(self):
        a = A()
        method, args, return_val = 'foo', {'args': 'foo_args'}, 'bar'
        a.call = Mock(return_value=return_val)
        a.cast = Mock()

        # when call_or_cast is invoke with default arguments:
        # call is invoked, result is returned
        result = a.call_or_cast(method, args)
        a.call.assert_called_once_with(method, args)
        self.assertEquals(result, return_val)
        a.call.reset_mock()

        # when call_or_cast is invoke with nowait=True:
        # call is invoked, no result is returned
        result = a.call_or_cast(method, args, nowait=False)
        a.call.assert_called_once_with(method, args)
        self.assertEquals(result, return_val)
        a.call.reset_mock()

        # when call_or_cast is invoke with nowait=True:
        # cast is invoked, no result is returned
        result = a.call_or_cast(method, args, nowait=True)
        a.cast.assert_called_once_with(method, args)

    @patch('cell.actors.uuid')
    def test_call(self, new_uuid):
        dummy_method, dummy_args, ticket = 'foo', {'foo': 1}, '12345'
        new_uuid.return_value = ticket

        a = A()
        a.cast = Mock()

        # when call is invoked:
        # cast is invoked with correct reply_to argument
        res = a.call(dummy_method, dummy_args)
        self.assertTrue(a.cast.called)
        (method, args, _), kwargs = a.cast.call_args
        self.assertEqual(method, dummy_method)
        self.assertEqual(args, dummy_args)
        self.assertDictContainsSubset({'reply_to': ticket}, kwargs)

        # returned result is correct
        self.assertIsInstance(res, AsyncResult)
        self.assertEquals(res.ticket, ticket)

#-----------------------------------------------------------------
# Test the cast method
#-----------------------------------------------------------------

    @patch('kombu.transport.memory.Channel.basic_publish')
    def assert_cast_calls_basic_publish_with(self, a, routing_key,
                                             exchange, type, mocked_publish):
            method, args = 'foo', {'bar': 'foo_arg'}
            type = type or ACTOR_TYPE.DIRECT
            ticket = uuid()
            expected_body, _ = get_test_message(
                method, args, a.__class__.__name__, reply_to=ticket)

            a.cast(method, args, type=type)

            mocked_publish.assert_called_once_with(
                ANY, immediate=ANY, mandatory=ANY,
                exchange=exchange, routing_key=routing_key)

            (message, ), _ = mocked_publish.call_args
            body = ast.literal_eval(message.get('body'))
            self.assertDictEqual(body, expected_body)

    @with_in_memory_connection
    def test_cast_calls_basic_publish_with_correct_exchange(self, conn):
        a = A(conn)
        rk = a.routing_key

        direct_exchange = a.type_to_exchange[ACTOR_TYPE.DIRECT]()
        self.assert_cast_calls_basic_publish_with(
            a, rk, direct_exchange.name, ACTOR_TYPE.DIRECT)

        fanout_exchange = a.type_to_exchange[ACTOR_TYPE.SCATTER]()
        self.assert_cast_calls_basic_publish_with(
            a, rk, fanout_exchange.name, ACTOR_TYPE.SCATTER)

        rr_exchange = a.type_to_exchange[ACTOR_TYPE.RR]()
        self.assert_cast_calls_basic_publish_with(
            a, rk, rr_exchange.name, ACTOR_TYPE.RR)

    @with_in_memory_connection
    def test_cast_calls_basic_publish_with_correct_rk(self, conn):
        a = A(conn)
        exch = a.type_to_exchange[ACTOR_TYPE.DIRECT]().name

        a = A(conn)
        rk = a.routing_key
        self.assert_cast_calls_basic_publish_with(a, rk, exch, None)

        agent = Mock(id=uuid())
        a = A(conn, agent=agent)
        rk = a.routing_key

        self.assertNotEqual(a.routing_key, agent.id)
        self.assert_cast_calls_basic_publish_with(a, rk, exch, None)

        agent = Mock(id=uuid())
        id = '1234'
        a = A(conn, agent=agent, id=id)
        rk = a.routing_key

        self.assertEqual(a.routing_key, id)
        self.assert_cast_calls_basic_publish_with(a, rk, exch, None)

        self.assertEquals(a.routing_key, rk)
        self.assert_cast_calls_basic_publish_with(a, rk, exch, None)

        a = A(conn)
        a.default_routing_key = 'fooooooooo'
        rk = a.default_routing_key

        self.assertEquals(a.routing_key, rk)
        self.assert_cast_calls_basic_publish_with(a, rk, exch, None)

    @with_in_memory_connection
    def test_cast_not_supported_type(self, conn):
        a = A(conn)
        with self.assertRaises(Exception):
            a.cast(method='foo', args={}, type='my_type')

    @with_in_memory_connection
    def test_cast_direct(self, conn):
        a = A(conn)
        b = A(conn)
        data_no_args = {'method': 'foo', 'args': {},
                        'class': a.__class__.__name__}

        data_with_args = {'method': 'foo', 'args': {'foo': 'foo_arg'},
                          'class': a.__class__.__name__}

        a_con = a.Consumer(conn.channel())
        b_con = b.Consumer(conn.channel())

        # when cast is invoked with default arguments:
        # the message is delivered only to its actor and to no one else,
        a.cast(method=data_no_args['method'], args=data_no_args['args'])
        self.assertNextMsgDataEqual(a_con, data_no_args)
        self.assertIsNone(get_next_msg(b_con))

        # when cast is invoked with type = ACTOR_TYPE_DIRECT:
        # the message is delivered only to its actor and to no one else,
        a.cast(method=data_with_args['method'], args=data_with_args['args'],
               type=ACTOR_TYPE.DIRECT)
        self.assertNextMsgDataEqual(a_con, data_with_args)
        self.assertIsNone(get_next_msg(b_con))

        clean_up_consumers([a_con, b_con])

    @with_in_memory_connection
    def test_cast_scatter(self, conn):

        class AnotherRRActor(Actor):
            type = (ACTOR_TYPE.RR, )

        a = ScatterActor(conn)
        data_with_args = {'method': 'foo', 'args': {'foo': 'foo_arg'},
                          'class': a.__class__.__name__}

        b, c = ScatterActor(conn), A(conn)
        d, e = RRActor(conn), AnotherRRActor(conn)
        a_con = a.Consumer(conn.channel())
        b_con = b.Consumer(conn.channel())
        c_con = c.Consumer(conn.channel())
        d_con = d.Consumer(conn.channel())
        e_con = e.Consumer(conn.channel())

        # when cast is invoked for broadcast:
        # all consumers for that actor class get the message and
        # the message are not consumed by consumers for other actor classes
        a.cast(method=data_with_args['method'], args=data_with_args['args'],
               type=ACTOR_TYPE.SCATTER)
        self.assertNextMsgDataEqual(a_con, data_with_args)
        self.assertNextMsgDataEqual(b_con, data_with_args)
        self.assertIsNone(get_next_msg(c_con))
        self.assertIsNone(get_next_msg(d_con))
        self.assertIsNone(get_next_msg(e_con))

        clean_up_consumers([a_con, b_con, c_con, d_con, e_con])

    @with_in_memory_connection
    def test_cast_round_robin_send_once(self, conn):
        # when cast is invoked once,
        # excatly one consumer should receive the message
        a, b, c = RRActor(conn), RRActor(conn), A()
        data_with_args = {'method': 'foo', 'args': {'foo': 'foo_arg'},
                          'class': a.__class__.__name__}

        a_con = a.Consumer(conn.channel())
        b_con = b.Consumer(conn.channel())
        c_con = c.Consumer(conn.channel())

        # when cast is invoked for round-robin:
        # only one consumer for that actor class receives the message and
        # messages are consumed by consumers for other actor classes
        a.cast(method=data_with_args['method'], args=data_with_args['args'],
               type=ACTOR_TYPE.RR)

        a_msg = get_next_msg(a_con)
        b_msg = get_next_msg(b_con)
        self.assertTrue((a_msg or b_msg) and (not(a_msg and b_msg)))
        self.assertIsNone(get_next_msg(c_con))

        clean_up_consumers([a_con, b_con, c_con])

    @with_in_memory_connection
    def test_cast_round_robin_send_repeatedly(self, conn):
        # when cast is invoked many time,
        # eventually all consumers should consume at least one message
        a, b, c = RRActor(conn), RRActor(conn), A()
        data_with_args = {'method': 'foo', 'args': {'foo': 'foo_arg'},
                          'class': a.__class__.__name__}

        a_con = a.Consumer(conn.channel())
        b_con = b.Consumer(conn.channel())
        c_con = c.Consumer(conn.channel())

        for i in range(1, 5):
            a.cast(method=data_with_args['method'],
                   args=data_with_args['args'],
                   type=ACTOR_TYPE.RR)

        self.assertNextMsgDataEqual(a_con, data_with_args)
        self.assertNextMsgDataEqual(b_con, data_with_args)
        self.assertIsNone(get_next_msg(c_con))

        clean_up_consumers([a_con, b_con, c_con])

#-----------------------------------------------------------------
# Test functionality for correct dispatch of method calls
#--------------------------------------------------------------------------
    def test_on_message_when_reply_to_is_set(self):

        class Foo(Actor):
            class state():
                foo_called = False

                def foo(self, bar):
                    self.foo_called = True
                    return (bar, ret_val)

        args, ret_val = {'bar': 'foo_arg'}, 'foooo'
        ticket = uuid()
        body, message = get_test_message(
            'foo', args, Foo.__class__.__name__, reply_to=[ticket])

        a = Foo()
        a.reply = Mock()

        # when the property reply_to is set, reply is called
        a._on_message(body, message)

        self.assertTrue(a.state.foo_called)
        a.reply.assert_called_oncce()

    def test_on_message_when_reply_to_not_set(self):
        ret_val = 'fooo'

        class Foo(Actor):
            class state():
                foo_called = False

                def foo(self, bar):
                    self.foo_called = True
                    return (bar, ret_val)

        # when the property reply_to is not set, reply is not called
        body, message = get_test_message(
            'foo', {'bar': 'foo_arg'}, Foo.__class__.__name__)
        message.ack = Mock()
        a = Foo()
        a.reply = Mock()

        result = a._on_message(body, message)

        self.assertTrue(a.state.foo_called)
        self.assertEquals(a.reply.call_count, 0)
        # message should be acknowledged after the method is executed
        message.ack.assert_called_once()
        # no result should be returned
        self.assertIsNone(result)

    def test_on_message_invokes_on_dispatch_when_reply_to_not_set(self):
        ret_val = 'fooo'
        body, message = get_test_message('foo', {'bar': 'foo_arg'},
                                         A.__class__.__name__)
        a = A()
        a.reply = Mock()
        a._DISPATCH = Mock(return_value=ret_val)

        # when reply_to is not set:
        # dispatch result should be ignored
        result = a._on_message(body, message)

        a._DISPATCH.assert_called_once_wiith(message, body)
        self.assertIsNone(result)
        self.assertEqual(a.reply.call_count, 0)

    def test_on_message_invokes_on_dispatch_when_reply_to_set(self):
        ret_val = 'fooo'
        ticket = uuid()
        body, message = get_test_message('foo', {'bar': 'foo_arg'},
                                         A.__class__.__name__,
                                         reply_to=ticket)
        a = A()
        a.reply = Mock()
        a._DISPATCH = Mock(return_value=ret_val)

        # when reply_to is set:
        # dispatch result should be ignored
        a._on_message(body, message)

        a._DISPATCH.assert_called_once_with(body, ticket=ticket)
        a.reply.assert_called_once_with(message, ret_val)

    def test_on_message_when_no_method_is_passed(self):
        args, ret_val = {'bar': 'foo_arg'}, 'fooo'

        class Foo(Actor):
            class state():
                def foo(self, bar):
                    self.foo_called = True
                    return (bar, ret_val)

        body, message = get_test_message('', {'bar': 'foo_arg'},
                                         Foo.__class__.__name__)
        message.ack = Mock()
        a = Foo()
        a.default_receive = Mock()

        result = a._on_message(body, message)

        a.default_receive.assert_called_once(args)
        # message should be acknowledged even when the method does not exist
        message.ack.assert_called_once_with()
        self.assertIsNone(result)

    def test_on_message_when_private_method_is_passed(self):
        body, message = get_test_message('_foo', {},
                                         A.__class__.__name__)

        message.ack = Mock()
        a = A()
        a.state._foo = Mock()

        a._on_message(body, message)

        self.assertEqual(a.state._foo.call_count, 0)
        # message should be acknowledged even when method is not invoked
        message.ack.assert_called_once_with()

    def test_on_message_when_unexisted_method_is_passed(self):
        args, ret_val = {'bar': 'foo_arg'}, 'fooo'

        body, message = get_test_message('bar', {'bar': 'foo_arg'},
                                         A.__class__.__name__)
        message.ack = Mock()
        a = A()
        a.default_receive = Mock()

        result = a._on_message(body, message)

        # message should be acknowledged even when the method does not exist
        message.ack.assert_called_once_with()
        self.assertIsNone(result)

    def test_on_message_delegated_to_agent(self):
        body, message = get_test_message('bar', {'bar': 'foo_arg'},
                                         A.__class__.__name__)
        a = A()
        a.agent = Mock()
        a.on_message(body, message)

        a.agent.process_message.assert_called_once_with(a, body, message)

    def assert_on_message_exception_raise(self, exception_cls, ack_count):
        body, message = get_test_message('bar', {'bar': 'foo_arg'},
                                         A.__class__.__name__)
        a = A()
        message.ack = Mock()

        a.handle_cast = Mock(side_effect=exception_cls('Boom'))

        with self.assertRaises(exception_cls):
            a._on_message(body, message)
            self.assertEquals(message.ack.call_count, ack_count)

        a.handle_cast.reset_mock()
        message.ack.reset_mock()

        message.ack = Mock()
        a.handle_call = Mock(side_effect=exception_cls('Boom'))
        body, message = get_test_message('bar', {'bar': 'foo_arg'},
                                         A.__class__.__name__,
                                         reply_to=[uuid])

        with self.assertRaises(exception_cls):
            a._on_message(body, message)
            self.assertEquals(message.ack.call_count, ack_count)

    def test_on_message_when_base_exception_occurs(self):
        # Do not ack the message if an exceptional error occurs,
        self.assert_on_message_exception_raise(Exception, 0)
        # but do ack the message if BaseException
        # (SystemExit or KeyboardInterrupt)
        # is raised, as this is probably intended.
        self.assert_on_message_exception_raise(BaseException, 1)

    def test_dispatch_return_values(self):
        """In the case of a successful call the return value will
        be::

            {'ok': return_value, **default_fields}

        If the method raised an exception the return value
        will be::

            {'nok': [repr exc, str traceback], **default_fields}

        :raises KeyError: if the method specified is unknown
        or is a special method (name starting with underscore).
        """

        # when result is correct
        ret_val = 'foooo'
        a = A()
        body, message = get_test_message('bar', {'bar': 'foo_arg'},
                                         a.__class__.__name__)

        expected_result = {'ok': ret_val}
        a.state.bar = Mock(return_value=ret_val)

        result = a._DISPATCH(body)

        self.assertDictContainsSubset(expected_result, result)
        self.assertNotIn('nok', result)

        # when method called does not return a result
        a.state.bar.reset_mock()
        a.state.bar = Mock(return_value=None)
        expected_result = {'ok': None}

        result = a._DISPATCH(body)

        self.assertDictContainsSubset(expected_result, result)
        self.assertNotIn('nok', result)

        # when method does not exist
        body, message = get_test_message(
            'foo', {'bar': 'foo_arg'}, a.__class__.__name__)

        result = a._DISPATCH(body)

        self.assertIn('nok', result)
        self.assertIn("KeyError('foo',)", result['nok'])

        # when calling a private method
        body, message = get_test_message(
            '_foo', {'bar': 'foo_arg'}, a.__class__.__name__)

        a._foo = Mock()
        result = a._DISPATCH(body)

        self.assertIn('nok', result)
        self.assertIn("KeyError('_foo',)", result['nok'])

        # when calling a private method
        body, message = get_test_message(
            '__foo', {'bar': 'foo_arg'}, a.__class__.__name__)

        a.__foo = Mock()
        result = a._DISPATCH(body)

        self.assertIn('nok', result)
        self.assertIn("KeyError('__foo',)", result['nok'])

        # when method called raises an exception
        body, message = get_test_message('foo_with_exception',
                                         {'bar': 'foo_arg'},
                                         a.__class__.__name__)

        a.foo_with_exception = Mock(side_effect=Exception('FooError'))
        result = a._DISPATCH(body)

        self.assertIn('nok', result)
        self.assertIn("KeyError('foo_with_exception',)", result['nok'])

    @with_in_memory_connection
    def test_on_message_is_sending_to_reply_queue(self, conn):
        ret_result = 'foooo'

        class Foo(A):
            class state:
                def bar(self, my_bar):
                    return ret_result

        a = Foo(conn)
        ticket = uuid()
        delivery_tag = uuid()
        body, message = get_encoded_test_message('bar', {'my_bar': 'bar_arg'},
                                                 A.__class__.__name__,
                                                 reply_to=ticket,
                                                 delivery_tag=delivery_tag)

        # Set up a reply queue to read from
        # reply_q and reply_exchange should be set the sender
        a.reply_exchange = a.reply_exchange.bind(a.connection.default_channel)
        maybe_declare(a.reply_exchange)
        reply_q = a.get_reply_queue(ticket)
        reply_q(a.connection.default_channel).declare()

        a._on_message(body, message)

        a_con = Consumer(conn.channel(), reply_q)
        self.assertNextMsgDataEqual(a_con, {'ok': ret_result})

    @with_in_memory_connection
    def test_reply_queue_is_declared_after_call(self, conn):
        ticket = uuid()
        with patch('cell.actors.uuid') as new_uuid:
            new_uuid.return_value = ticket

            a = A(conn)
            reply_q = a.get_reply_queue(ticket)
            a.get_reply_queue = Mock(return_value=reply_q)

            with self.assertRaises(StdChannelError):
                reply_q(conn.channel()).queue_declare(passive=True)

            a.call(method='foo', args={}, type=ACTOR_TYPE.DIRECT)

            a.get_reply_queue.assert_called_once_with(ticket)
            self.assertTrue(
                reply_q(conn.channel()).queue_declare(passive=True))

    @with_in_memory_connection
    def test_reply_send_correct_msg_body_to_the_reply_queue(self, conn):
        a = A(conn)
        ticket = uuid()
        delivery_tag = 2
        body, message = get_encoded_test_message('bar', {'my_bar': 'bar_arg'},
                                                 a.__class__.__name__,
                                                 reply_to=ticket,
                                                 delivery_tag=delivery_tag)

        # Set up a reply queue to read from
        # reply_q and reply_exchange should be set the sender
        a.reply_exchange.maybe_bind(a.connection.default_channel)
        maybe_declare(a.reply_exchange)
        reply_q = a.get_reply_queue(ticket)
        reply_q(a.connection.default_channel).declare()

        a.reply(message, body)

        a_con = Consumer(conn.channel(), reply_q)
        reply_msg = get_next_msg(a_con)
        reply_body = reply_msg.decode()
        self.assertEquals(reply_body, body)

#-----------------------------------------------------------------
# Test actor to actor binding functionality (add_binding, remove_binding)
# ----------------------------------------------------------------

    def mock_exchange(self, actor, type):
        exchange = actor.type_to_exchange[type]()
        exchange.bind_to = Mock()
        exchange.exchange_unbind = Mock()
        exchange.declare = Mock()
        actor.type_to_exchange[type] = Mock(return_value=exchange)

        return exchange

    def mock_queue(self, actor, type):
        queue = actor.type_to_queue[type]()
        queue.bind_to = Mock()
        queue.unbind_from = Mock()
        queue.declare = Mock()
        actor.type_to_queue[type] = Mock(return_value=queue)

        return queue

    @with_in_memory_connection
    def test_add_remove_binding_for_direct_type(self, conn):
        # Add binding between the inbox queue
        # of one actor to the outbox queue of another
        a, b = A(conn), A(conn)
        routing_key = 'foooo'
        mock_entity_type = ACTOR_TYPE.DIRECT
        inbox_queue = self.mock_queue(a, mock_entity_type)
        source_ex = b.outbox

        a._add_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        inbox_queue.bind_to.assert_called_with(
            exchange=b.outbox, routing_key=routing_key)

        a._remove_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        inbox_queue.unbind_from.assert_called_with(
            exchange=source_ex, routing_key=routing_key)

    @with_in_memory_connection
    def test_add_remove_binding_for_scatter_type(self, conn):
        a, b = A(conn), A(conn)
        routing_key, mock_entity_type = 'foooo', ACTOR_TYPE.SCATTER

        dest_ex = self.mock_exchange(a, mock_entity_type)
        source_ex = b.outbox

        a._add_binding(source_ex.as_dict(),
                       routing_key=routing_key,
                       inbox_type=mock_entity_type)

        dest_ex.bind_to.assert_called_with(exchange=source_ex,
                                           routing_key=routing_key)

        a._remove_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        dest_ex.exchange_unbind.assert_called_with(
            exchange=source_ex, routing_key=routing_key)

    @with_in_memory_connection
    def test_add_remove_binding_for_rr_type(self, conn):
        a, b = A(conn), A(conn)
        routing_key, mock_entity_type = 'foooo', ACTOR_TYPE.RR
        dest_exchange = self.mock_exchange(a, mock_entity_type)
        source_ex = b.outbox

        a._add_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        dest_exchange.bind_to.assert_called_with(
            exchange=source_ex, routing_key=routing_key)

        a._remove_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        dest_exchange.exchange_unbind.assert_called_with(
            exchange=source_ex, routing_key=routing_key)

    @with_in_memory_connection
    def test_add_binding_when_actor_for_not_supported_type(self, conn):
        a, b = A(conn), A(conn)
        entity_type = 'test'

        self.assertNotIn(entity_type, a.types)
        with self.assertRaises(Exception):
            a._add_binding(b.outbox.as_dict(),
                           routing_key=b.routing_key, inbox_type=entity_type)

    @with_in_memory_connection
    def test_add_remove_binding_when_routing_key_is_empty(self, conn):
        a, b = A(conn), A(conn)
        routing_key, mock_entity_type = "", ACTOR_TYPE.SCATTER
        source_ex = Exchange('bar.foo.bar', mock_entity_type)

        exchange = self.mock_exchange(a, mock_entity_type)

        a._add_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        exchange.bind_to.assert_called_with(exchange=source_ex,
                                            routing_key=routing_key)

        a._remove_binding(source_ex.as_dict(), routing_key, mock_entity_type)

        exchange.exchange_unbind.assert_called_with(exchange=source_ex,
                                                    routing_key=routing_key)


class As(Actor):
    class state():
        def foo(self, who=None):
            pass

    def meth(self):
        pass


class test_ActorProxy(Case):

    @with_in_memory_connection
    def test_init(self, conn):
        """test that __init__ sets fields"""
        id = uuid()
        ag, res = Mock(), Mock()
        # we need to have wait for result,
        a1 = ActorProxy(qualname(A), id, connection=conn, agent=ag)

        self.assertEqual(a1.id, id)
        self.assertIsNone(a1.async_start_result)
        self.assertIsInstance(a1._actor, A)
        self.assertEqual(a1._actor.name, A().__class__.__name__)
        self.assertEqual(a1._actor.agent, ag)
        self.assertEqual(a1._actor.id, a1.id)
        self.assertEqual(a1._actor.connection, conn)

        a1 = ActorProxy(qualname(A), id, res, connection=conn, agent=ag)

        self.assertEqual(a1.id, id)
        self.assertEqual(a1.async_start_result, res)
        self.assertEqual(a1._actor.id, a1.id)
        self.assertIsInstance(a1._actor, A)
        self.assertEqual(a1._actor.name, A().__class__.__name__)
        self.assertEqual(a1._actor.agent, ag)
        self.assertEqual(a1._actor.connection, conn)

    def assert_actor_method_called(self, meth, func):
        args,  = ['foo', {'who': 'the quick brown...'}]
        kwargs = {'nowait': True}

        func(*args, **kwargs)

        meth.assert_called_once_with(*args, **kwargs)

        args = ['bar', {'who': 'the quick brown...'}]
        kwargs = {'nowait': True}

        with self.assertRaises(AttributeError):
            func(*args, **kwargs)

        with self.assertRaises(WrongNumberOfArguments):
            func()

    def assert_actor_method_called_with_par_foo(
            self, mock_meth, func):

        args, kwargs = [{'who': 'the quick brown...'}], {'nowait': True}

        func.foo(*args, **kwargs)

        mock_meth.assert_called_once_with('foo', *args, **kwargs)

        with self.assertRaises(AttributeError):
            func.bar(*args, **kwargs)

    @patch.object(Actor, 'call', return_value=None)
    def test_call_dot(self, call):
        a1 = ActorProxy(qualname(As), uuid)
        self.assert_actor_method_called_with_par_foo(call, a1.call)

    @patch.object(Actor, 'call', return_value=None)
    def test_call(self, call):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called(call, a1.call)

    @patch.object(Actor, 'send', return_value=None)
    def test_send(self, send):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called(send, a1.send)

    @patch.object(Actor, 'send', return_value=None)
    def test_send_dot(self, send):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called_with_par_foo(send, a1.send)

    @patch.object(Actor, 'throw', return_value=None)
    def test_throw(self, throw):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called(throw, a1.throw)

    @patch.object(Actor, 'throw', return_value=None)
    def test_throw_dot(self, throw):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called_with_par_foo(throw, a1.throw)

    @patch.object(Actor, 'scatter', return_value=None)
    def test_scatter(self, scatter):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called(scatter, a1.scatter)

    @patch.object(Actor, 'scatter', return_value=None)
    def test_scatter_dot(self, scatter):
        a1 = ActorProxy(qualname(As), uuid())
        self.assert_actor_method_called_with_par_foo(scatter, a1.scatter)

    @patch.object(As, 'meth', return_value=None)
    def test_arbitrary_actor_method(self, meth):
        a1 = ActorProxy(qualname(As), uuid())
        a1.meth()

        meth.assert_called_once_with()
        meth.reset_mock()

        args = ['bar']

        a1.meth(*args)
        meth.assert_called_once_with(*args)

    def test_non_existing_actor_method(self):
        a1 = ActorProxy(qualname(As), uuid())
        with self.assertRaises(AttributeError):
            a1.bar()
