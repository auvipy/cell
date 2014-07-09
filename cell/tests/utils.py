from __future__ import absolute_import
from kombu.connection import Connection

try:
    import unittest
    unittest.skip
    from unittest.util import safe_repr, unorderable_list_difference
except AttributeError:
    import unittest2 as unittest
    from unittest2.util import safe_repr, unorderable_list_difference  # noqa

import importlib
import os
import platform
import re
import sys
import warnings

from contextlib import contextmanager
from functools import partial, wraps
from types import ModuleType

import mock
from nose import SkipTest
from kombu.five import builtins, items, string_t, reraise, values, WhateverIO
from kombu.utils import nested


class Mock(mock.Mock):

    def __init__(self, *args, **kwargs):
        attrs = kwargs.pop('attrs', None) or {}
        super(Mock, self).__init__(*args, **kwargs)
        for attr_name, attr_value in items(attrs):
            setattr(self, attr_name, attr_value)


def skip_unless_module(module):

    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            try:
                importlib.import_module(module)
            except ImportError:
                raise SkipTest('Does not have %s' % (module, ))

            return fun(*args, **kwargs)

        return __inner
    return _inner


# -- adds assertWarns from recent unittest2, not in Python 2.7.

class _AssertRaisesBaseContext(object):

    def __init__(self, expected, test_case, callable_obj=None,
                 expected_regex=None):
        self.expected = expected
        self.failureException = test_case.failureException
        self.obj_name = None
        if isinstance(expected_regex, string_t):
            expected_regex = re.compile(expected_regex)
        self.expected_regex = expected_regex


class _AssertWarnsContext(_AssertRaisesBaseContext):
    """A context manager used to implement TestCase.assertWarns* methods."""

    def __enter__(self):
        # The __warningregistry__'s need to be in a pristine state for tests
        # to work properly.
        warnings.resetwarnings()
        for v in values(sys.modules):
            if getattr(v, '__warningregistry__', None):
                v.__warningregistry__ = {}
        self.warnings_manager = warnings.catch_warnings(record=True)
        self.warnings = self.warnings_manager.__enter__()
        warnings.simplefilter('always', self.expected)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.warnings_manager.__exit__(exc_type, exc_value, tb)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return
        try:
            exc_name = self.expected.__name__
        except AttributeError:
            exc_name = str(self.expected)
        first_matching = None
        for m in self.warnings:
            w = m.message
            if not isinstance(w, self.expected):
                continue
            if first_matching is None:
                first_matching = w
            if (self.expected_regex is not None and
                    not self.expected_regex.search(str(w))):
                continue
            # store warning for later retrieval
            self.warning = w
            self.filename = m.filename
            self.lineno = m.lineno
            return
        # Now we simply try to choose a helpful failure message
        if first_matching is not None:
            raise self.failureException(
                '%r does not match %r'
                % (self.expected_regex.pattern, str(first_matching)))
        if self.obj_name:
            raise self.failureException('%s not triggered by %s'
                                        % (exc_name, self.obj_name))
        else:
            raise self.failureException('%s not triggered'
                                        % exc_name)


class Case(unittest.TestCase):

    def assertWarns(self, expected_warning):
        return _AssertWarnsContext(expected_warning, self, None)

    def assertWarnsRegex(self, expected_warning, expected_regex):
        return _AssertWarnsContext(expected_warning, self,
                                   None, expected_regex)

    def assertDictContainsSubset(self, expected, actual, msg=None):
        missing, mismatched = [], []

        for key, value in items(expected):
            if key not in actual:
                missing.append(key)
            elif value != actual[key]:
                mismatched.append('%s, expected: %s, actual: %s' % (
                    safe_repr(key), safe_repr(value),
                    safe_repr(actual[key])))

        if not (missing or mismatched):
            return

        standard_msg = ''
        if missing:
            standard_msg = 'Missing: %s' % ','.join(map(safe_repr, missing))

        if mismatched:
            if standard_msg:
                standard_msg += '; '
            standard_msg += 'Mismatched values: %s' % (
                ','.join(mismatched))

        self.fail(self._formatMessage(msg, standard_msg))

    def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
        try:
            expected = sorted(expected_seq)
            actual = sorted(actual_seq)
        except TypeError:
            # Unsortable items (example: set(), complex(), ...)
            expected = list(expected_seq)
            actual = list(actual_seq)
            missing, unexpected = unorderable_list_difference(
                expected, actual)
        else:
            return self.assertSequenceEqual(expected, actual, msg=msg)

        errors = []
        if missing:
            errors.append('Expected, but missing:\n    %s' % (
                          safe_repr(missing)))
        if unexpected:
            errors.append('Unexpected, but present:\n    %s' % (
                          safe_repr(unexpected)))
        if errors:
            standardMsg = '\n'.join(errors)
            self.fail(self._formatMessage(msg, standardMsg))


def with_in_memory_connection(fn):

        @wraps(fn)
        def wrapper(self, *args, **kwargs):
                with Connection('memory://') as conn:
                    fn(self, conn, *args, **kwargs)
        return wrapper


def with_environ(env_name, env_value):

    def _envpatched(fun):

        @wraps(fun)
        def _patch_environ(*args, **kwargs):
            prev_val = os.environ.get(env_name)
            os.environ[env_name] = env_value
            try:
                return fun(*args, **kwargs)
            finally:
                if prev_val is not None:
                    os.environ[env_name] = prev_val

        return _patch_environ
    return _envpatched


def patch(module, name, mocked):
    module = importlib.import_module(module)

    def _patch(fun):

        @wraps(fun)
        def __patched(*args, **kwargs):
            prev = getattr(module, name)
            setattr(module, name, mocked)
            try:
                return fun(*args, **kwargs)
            finally:
                setattr(module, name, prev)
        return __patched
    return _patch


@contextmanager
def replace_module_value(module, name, value=None):
    has_prev = hasattr(module, name)
    prev = getattr(module, name, None)
    if value:
        setattr(module, name, value)
    else:
        try:
            delattr(module, name)
        except AttributeError:
            pass
    yield
    if prev is not None:
        setattr(sys, name, prev)
    if not has_prev:
        try:
            delattr(module, name)
        except AttributeError:
            pass
pypy_version = partial(
    replace_module_value, sys, 'pypy_version_info',
)
platform_pyimp = partial(
    replace_module_value, platform, 'python_implementation',
)


@contextmanager
def sys_platform(value):
    prev, sys.platform = sys.platform, value
    yield
    sys.platform = prev


@contextmanager
def mock_module(*names):
    prev = {}

    class MockModule(ModuleType):

        def __getattr__(self, attr):
            setattr(self, attr, Mock())
            return ModuleType.__getattribute__(self, attr)

    mods = []
    for name in names:
        try:
            prev[name] = sys.modules[name]
        except KeyError:
            pass
        mod = sys.modules[name] = MockModule(name)
        mods.append(mod)
    try:
        yield mods
    finally:
        for name in names:
            try:
                sys.modules[name] = prev[name]
            except KeyError:
                try:
                    del(sys.modules[name])
                except KeyError:
                    pass


@contextmanager
def mock_context(mock, typ=Mock):
    context = mock.return_value = Mock()
    context.__enter__ = typ()
    context.__exit__ = typ()

    def on_exit(*x):
        if x[0]:
            reraise(x[0], x[1], x[2])
    context.__exit__.side_effect = on_exit
    context.__enter__.return_value = context
    yield context
    context.reset()


@contextmanager
def mock_open(typ=WhateverIO, side_effect=None):
    with mock.patch('__builtin__.open') as open_:
        with mock_context(open_) as context:
            if side_effect is not None:
                context.__enter__.side_effect = side_effect
            val = context.__enter__.return_value = typ()
            val.__exit__ = Mock()
            yield val


def patch_many(*targets):
    return nested(*[mock.patch(target) for target in targets])


def skip_if_pypy(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        if getattr(sys, 'pypy_version_info', None):
            raise SkipTest('does not work on PyPy')
        return fun(*args, **kwargs)
    return _inner


def skip_if_jython(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        if sys.platform.startswith('java'):
            raise SkipTest('does not work on Jython')
        return fun(*args, **kwargs)
    return _inner
