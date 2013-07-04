"""cell.bin.base"""

from __future__ import absolute_import

import optparse
import os
import sys

from cell import __version__

__all__ = ['Option', 'Command']
Option = optparse.make_option


class Command(object):
    Parser = optparse.OptionParser

    args = ''
    version = __version__
    option_list = ()
    prog_name = None

    def run(self, *args, **options):
        raise NotImplementedError('subclass responsibility')

    def execute_from_commandline(self, argv=None):
        """Execute application from command line.

        :keyword argv: The list of command line arguments.
                       Defaults to ``sys.argv``.

        """
        if argv is None:
            argv = list(sys.argv)
        self.prog_name = os.path.basename(argv[0])
        return self.handle_argv(self.prog_name, argv[1:])

    def usage(self):
        """Returns the command-line usage string for this app."""
        return '%%prog [options] %s' % (self.args, )

    def get_options(self):
        """Get supported command line options."""
        return self.option_list

    def handle_argv(self, prog_name, argv):
        """Parses command line arguments from ``argv`` and dispatches
        to :meth:`run`.

        :param prog_name: The program name (``argv[0]``).
        :param argv: Command arguments.

        """
        options, args = self.parse_options(prog_name, argv)
        return self.run(*args, **vars(options))

    def exit(self, v=0):
        sys.exit(v)

    def exit_status(self, msg, status=0, fh=sys.stderr):
        fh.write('%s\n' % (msg, ))
        self.exit(status)

    def exit_usage(self, msg):
        sys.stderr.write('ERROR: %s\n\n' % (msg, ))
        self.exit_status('Usage: %s' % (
            self.usage().replace('%prog', self.prog_name), ))

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        # Don't want to load configuration to just print the version,
        # so we handle --version manually here.
        if '--version' in arguments:
            self.exit_status(self.version, fh=sys.stdout)
        parser = self.create_parser(prog_name)
        options, args = parser.parse_args(arguments)
        return options, args

    def create_parser(self, prog_name):
        return self.Parser(prog=prog_name,
                           usage=self.usage(),
                           version=self.version,
                           option_list=self.get_options())
