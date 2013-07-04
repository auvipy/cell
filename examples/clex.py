import cell

from celery import current_app as celery


class BlenderActor(cell.Actor):
    types = ('direct', 'round-robin')

    def __init__(self, connection=None, *args, **kwargs):
        # - use celery's connection by default,
        # - means the agent must be started from where it has access
        # - to the celery config.
        super(BlenderActor, self).__init__(
            connection or celery.broker_connection(), *args, **kwargs)

    class state:
        # - The state class defines the messages the actor can handle,
        # - so a message with method 'render' will be dispatched to
        # - the render method.

        def render(self, blabla):
            print('communicates with the blender process here')
            return 'value'

    # - It is good practice to provide helper methods, so that
    # - clients don't have to use .call, etc directly

    def render(self, blabla, nowait=False):
        return self.throw('render', {'blabla': blabla}, nowait=nowait)
blender = BlenderActor()


class Agent(cell.Agent):
    actors = [blender]

    def __init__(self, connection=None, *args, **kwargs):
        # - use celery's connection by default
        super(Agent, self).__init__(
            connection or celery.broker_connection(), *args, **kwargs)


if __name__ == '__main__':
    Agent().run_from_commandline()


####
# Run this script in one console, and in another run the following:
#
#    >>> from x import blender
#
#    >>> # call and wait for result
#    >>> blender.render('foo').get()
#    'value'
#
#    >>> # send and don't care about result
#    >>> blender.render('foo', nowait=True)
