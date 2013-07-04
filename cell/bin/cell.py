"""cell.bin.cell"""

from __future__ import absolute_import

from kombu import Connection

from .base import Command, Option
from cell import Agent
from cell.utils import instantiate

__all__ = ['cell', 'main']

DEFAULT_BROKER_URL = 'amqp://guest:guest@localhost:5672//'


class cell(Command):
    args = '<agent object names>'

    option_list = (
        Option('-i', '--id',
               default=None, action='store', dest='id',
               help='Id of the agent (or automatically generated).'),
        Option('-l', '--loglevel',
               default=None, action='store', dest='loglevel',
               help='Loglevel (CRITICAL/ERROR/WARNING/INFO/DEBUG).'),
        Option('-f', '--logfile',
               default=None, action='store', dest='logfile',
               help='Logfile. Default is stderr.'),
        Option('-b', '--broker',
               default=DEFAULT_BROKER_URL, action='store', dest='broker',
               help='Broker URL. Default is %s' % (DEFAULT_BROKER_URL, )),
        )

    def run(self, *actors, **kwargs):
        if not actors:
            self.exit_usage('No actor specified')

        actors = [instantiate(actor) for actor in list(actors)]

        connection = Connection(kwargs.get('broker'))
        agent = Agent(connection, actors=actors, id=kwargs.get('id'))
        agent.run_from_commandline(loglevel=kwargs.get('loglevel'),
                                   logfile=kwargs.get('logfile'))


def main(argv=None):
    return cell().execute_from_commandline(argv)


if __name__ == '__main__':
    main()
