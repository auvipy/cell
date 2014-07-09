from __future__ import absolute_import

from cell.actors import Actor
from cell.agents import dAgent
from kombu.entity import Exchange

__author__ = 'rumi'


class Group(Actor):
    """Convenience class used to spawn group of actors of the same type.

    **Example usage**
     Here we spawn two groups, of 10 :class:`Logger` actors each.

    .. code-block:: python

        >>> exception_group = agent.spawn(Group, Logger, 10)
        >>> warning_group = agent.spawn(Group, Logger, 10)
        >>> exception_group.scatter('log_msg', 'some exception msg...')
        >>> warning_group.scatter('log_msg', 'some exception msg...')

    :param act_type: the actor to spawn.
    :param number: the number of actor instances to spawn.

    """
    def __init__(self, act_type, number, **kwargs):
        super(Group, self).__init__(**kwargs)
        self.state.act_type = act_type
        self.state.number = number

    class state(object):
        def config(self, act_type, number):
            agent = dAgent(self.actor.connection)

            for _ in range(0, number):
                agent.spawn(
                    act_type,
                    {'group_exchange': self.actor.inbox_scatter.name})

    def get_scatter_exchange(self):
        """Returns a :class:'kombu.Exchange' for type fanout"""
        return Exchange('cl.scatter.%s.%s' % (self.name, self.id),
                        'fanout', auto_delete=True)

    def on_agent_ready(self):
        self.state.config(self.state.act_type, self.state.number)

    def get_queues(self):
        return []
