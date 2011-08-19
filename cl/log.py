"""cl.log"""

from __future__ import absolute_import

import logging
import sys

from logging.handlers import WatchedFileHandler

from kombu.utils.encoding import safe_str

from .utils import cached_property

__all__ = ["LogMixin", "LOG_LEVELS", "get_loglevel", "setup_logger"]

LOG_LEVELS = dict(logging._levelNames)
LOG_LEVELS["FATAL"] = logging.FATAL
LOG_LEVELS[logging.FATAL] = "FATAL"


def get_loglevel(level):
    if isinstance(level, basestring):
        return LOG_LEVELS[level]
    return level


class LogMixin(object):

    def debug(self, *args, **kwargs):
        return self._log(logging.DEBUG, *args, **kwargs)

    def info(self, *args, **kwargs):
        return self._log(logging.INFO, *args, **kwargs)

    def warn(self, *args, **kwargs):
        return self._log(logging.WARN, *args, **kwargs)

    def error(self, *args, **kwargs):
        kwargs.setdefault("exc_info", sys.exc_info())
        return self._log(logging.ERROR, *args, **kwargs)

    def _log(self, severity, *args, **kwargs):
        body = "{%s} %s" % (self.logger_name, " ".join(map(safe_str, args)))
        return self.logger.log(severity, body, **kwargs)

    @cached_property
    def logger(self):
        return logging.getLogger(self.logger_name)

    @property
    def logger_name(self):
        return self.__class__.__name__


def setup_logger(loglevel=None, logfile=None):
    logger = logging.getLogger()
    loglevel = get_loglevel(loglevel or "ERROR")
    logfile = logfile if logfile else sys.__stderr__
    if not logger.handlers:
        if hasattr(logfile, "write"):
            handler = logging.StreamHandler(logfile)
        else:
            handler = logging.WatchedFileHandler(logfile)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
