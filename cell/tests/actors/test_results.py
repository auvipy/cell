from __future__ import absolute_import

from mock import ANY, patch
from cell.actors import Actor
from cell.agents import dAgent
from cell.exceptions import CellError, NoReplyError
from cell.results import AsyncResult
from cell.tests.utils import Case, Mock, with_in_memory_connection
from kombu.utils import uuid

__author__ = 'rumi'


class A(Actor):
    pass


class Ag(dAgent):
    def get_default_scatter_limit(self):
        return 5


class test_AsyncResuls(Case):
    def get_async_result(self):
        ticket = uuid()
        actor = Mock()
        ares = AsyncResult(ticket, actor)
        return ares

    def test_init(self):
        ticket = uuid()
        actor = Mock()
        ares = AsyncResult(ticket, actor)

        self.assertEquals(ares.ticket, ticket)
        self.assertEqual(ares.actor, actor)
        self.assertIsNone(ares._result)
        self.assertEqual(ares.Error, CellError)
        self.assertEqual(ares.NoReplyError, NoReplyError)

        with self.assertRaises(TypeError):
            AsyncResult(ticket)

    def test_result_when_result_is_set(self):
        val = 'the quick brown fox'
        ares = self.get_async_result()
        ares.get = Mock()
        ares._result = val

        res = ares.result()

        self.assertEqual(res, val)
        self.assertEqual(ares.get.call_count, 0)

    def test_result_when_result_is_not_set(self):
        val = 'the quick brown fox'
        ares = self.get_async_result()
        ares.get = Mock(return_value=val)

        res = ares.result()

        self.assertEqual(res, val)
        self.assertEqual(ares._result, res)
        ares.get.assert_called_once_with()

    def test_to_python(self):
        ok_message = {'ok': 'the quick_brown_fox'}
        ares = self.get_async_result()

        # ------------------------------
        # reply is a successful message
        # ------------------------------

        # correct format
        res = ares.to_python(ok_message)

        self.assertEqual(res, ok_message['ok'])

        # correct format with multiple keys in the reply dict
        ok_message = {'ok': 'the quick_brown_fox',
                      'foo': 'the quick_brown_fox'}
        res = ares.to_python(ok_message)
        self.assertEqual(res, ok_message['ok'])

        # contains both ok and nok
        ok_message = {'ok': 'the quick_brown_fox',
                      'nok': 'the quick_brown_fox'}
        res = ares.to_python(ok_message)
        self.assertEqual(res, ok_message['ok'])

        # ---------------------------
        # reply is an error message
        # ---------------------------

        # correct error format with to propagate param set
        error_message = {'nok': [Exception('jump over')]}
        with self.assertRaises(ares.Error):
            ares.to_python(error_message)

        # correct error format with to propagate set to True
        with self.assertRaises(ares.Error):
            ares.to_python(error_message, propagate=True)

        # correct error format with to propagate set to False
        error_message = {'nok': ['jump over', None]}
        res = ares.to_python(error_message, propagate=False)
        self.assertEquals(res.__dict__,
                          ares.Error(*error_message.get('nok')).__dict__)

        # neither nok or ok message given
        error_message = {'foo': ['jump over']}
        with self.assertRaises(ares.Error):
            ares.to_python(error_message)

        # multiple keys in the reply dics given, one of teh eks is nok
        error_message = {'foo': 'the quick_brown_fox',
                         'nok': ['jump over']}
        res = ares.to_python(error_message, propagate=False)
        self.assertEqual(res.__dict__,
                         ares.Error(*error_message['nok']).__dict__)

    def test_get(self):
        id1, id2 = uuid(), uuid()

        def gather():
            yield id1
            yield id2

        # test that it calls gather with limit = 1 and kwargs
        ares = self.get_async_result()
        ares.gather = Mock(return_value=['1'])

        ares.get()
        ares.gather.assert_called_once_with(limit=1)
        ares.gather.reset_mock()

        kwargs = {'timeout': 100, 'ignore_timeout': False,
                  'foo': 'bar', 'propaget': True}
        ares.get(**kwargs)
        ares.gather.assert_called_once_with(**dict(kwargs, limit=1))
        ares.gather.reset_mock()

        kwargs = {'timeout': 100, 'ignore_timeout': False, 'limit': 10}
        ares.get(**kwargs)
        ares.gather.assert_called_once_with(**kwargs)
        ares.gather.reset_mock()

        # it returns the first value of whatever gather returns
        ares.gather = Mock(return_value=gather())
        res = ares.get()
        self.assertEqual(res, id1)

        # if gather does not return result:
        # self.NoReplyError('No reply received within time constraint')
        ares.gather = Mock(return_value=None)

        with self.assertRaises(ares.NoReplyError):
            ares.get()

        ares.gather.reset_mock()
        ares.gather = Mock(return_value={})

        with self.assertRaises(ares.NoReplyError):
            ares.get()

    @with_in_memory_connection
    def test_gather(self, conn):
        def collect_replies():
            yield 1
            yield 2
            yield 3

        ticket = uuid()
        actor = Actor(conn)
        actor._collect_replies = Mock(return_value=collect_replies())

        ares = AsyncResult(ticket, actor)
        ares.to_python = Mock()

        all = ares.gather()
        list(all)

        actor._collect_replies.assert_caleld_once_with(conn, ANY, ticket)
        self.assertEqual(ares.to_python.call_count,
                         len(list(collect_replies())))

        # test that the to_python is applied to all results
        actor._collect_replies.reset_mock()
        actor._collect_replies = Mock(return_value=collect_replies())
        prev_to_python = ares.to_python
        new_to_python = lambda x, propagate = True: 'called_%s' % x
        ares.to_python = new_to_python

        all = ares.gather()
        vals = list(all)

        expected_vals = [new_to_python(i) for i in collect_replies()]

        actor._collect_replies.assert_caleld_once_with(conn, ANY, ticket)
        self.assertEqual(vals, expected_vals)
        ares.to_python = prev_to_python

        # test kwargs

    @patch('cell.actors.collect_replies')
    @with_in_memory_connection
    def test_gather_kwargs(self, conn, collect):
        actor = Actor(conn)
        ares = AsyncResult(uuid(), actor)
        prev_to_python = ares.to_python
        new_to_python = lambda x, propagate = True: x
        ares.to_python = new_to_python

        # Test default kwargs,
        # nothing is passed, the actor does not have agent assigned
        self.assert_gather_kwargs(
            ares, collect, {},
            timeout=actor.default_timeout, ignore_timeout=False)

        # limit - set the default agent limit if NONE is set
        # Test default kwargs, nothing is passed,
        # the actor does have default agent assigned
        actor.agent = dAgent(conn)
        self.assert_gather_kwargs(
            ares, collect, {},
            timeout=actor.default_timeout, limit=None, ignore_timeout=False)

        # limit - set the default agent limit if NONE is set
        # Test default kwargs, nothing is passed,
        # the actor does have agent with custom scatter limit assigned
        ag = Ag(conn)
        actor.agent = ag
        self.assert_gather_kwargs(
            ares, collect, {}, timeout=actor.default_timeout,
            limit=ag.get_default_scatter_limit())

        # pass all args
        actor.agent = Ag(conn)
        timeout, ignore_timeout, limit = 200.0, False, uuid()

        self.assert_gather_kwargs(
            ares, collect,
            {'timeout': timeout, 'ignore_timeout': ignore_timeout,
             'limit': limit},
            timeout=timeout, limit=limit, ignore_timeout=ignore_timeout)

        # ig ignore_tiemout is passed,
        # the custom logic for limit is not applies
        actor.agent = None
        timeout, ignore_timeout = 200.0, True
        self.assert_gather_kwargs(
            ares, collect,
            {'timeout': timeout, 'ignore_timeout': ignore_timeout},
            timeout=timeout, ignore_timeout=ignore_timeout)

        ares.to_python = prev_to_python

    def assert_gather_kwargs(self, ares, collect, args, **kwargs):
        def drain():
            yield 1
            yield 2
        collect.return_value = drain()
        all = ares.gather(**args)

        self.assertEqual(list(all), list(drain()))
        collect.assert_called_once_with(ANY, ANY, ANY, **kwargs)
        collect.reset_mock()
