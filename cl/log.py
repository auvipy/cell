import logging
import sys

from kombu.utils import cached_property
from kombu.utils.encoding import safe_str


class LogMixin(object):

    def debug(self, *args, **kwargs):
        return self._log(logging.DEBUG, *args, **kwargs)

    def info(self, *args, **kwargs):
        return self._log(logging.INFO, *args, **kwargs)

    def warn(self, *args, **kwargs):
        return self._log(logging.WARN, *args, **kwargs)

    def error(self, *args, **kwargs):
        return self._log(logging.ERROR, *args, **kwargs)

    def _log(self, severity, *args, **kwargs):
        body = "{%s} %s" % (self.name, " ".join(map(safe_str, args)))
        return self.logger.log(severity, body, **kwargs)

    @cached_property
    def logger(self):
        return logging.getLogger(self.logger_name)

    @property
    def logger_name(self):
        return self.__class__.__name__

