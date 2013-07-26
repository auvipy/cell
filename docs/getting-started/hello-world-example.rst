
Hello World
===================
Here I will demonstrate the main cell features going through a simple Hello World example.

.. contents::
    :local:

To understand what is going on, we will break the example into pieces.

Creating an actor
~~~~~~~~~~~~~~~~~

Actors are implemented by extending the :py:class:`Actor` class and then defining a series of supported methods.
If you want your actor to handle general method calls, then you can implement the default_receive method.
If you call a not supported method a {‘nok’} result will be returned.

.. code-block:: python

    from cell.actors import Actor

    class GreetingActor(Actor):
        class state:
            def greet(self, who='world'):
                return 'Hello %s' % who


Starting an actor
~~~~~~~~~~~~~~~~~~~~~~~

Running the celery worker server
--------------------------------
You need to have the the celery worker running to start an actor.
Celery worker has an internal component for actors management that is used for starting/stopping actors
instances.

The celery worker can be run as follows:

.. code-block:: bash

    $ celery worker

Starting an actor
-----------------------
First, we need to create an instance of :py:class:`dAgent` (distributed agent).
Agents are used for managing

.. code-block:: python

    from cell.agents import dAgent
    from kombu import Connection

    connection = Connection()
    actors_mng = dAgent(connection)
    actor = actors_mng.add_actor(GreetingActor())

:py:meth:`actors_mng.add_actor` accepts an actor instance and returns an :py:meth:`ActorProxy` that can be used for
 sending messages to this actor or the actors of the same type.


Calling a method
~~~~~~~~~~~~~~~~~


.. code-block:: python

    actor.call('greet')

The :py:meth:`greet` has been executed and you can verify that by the remote actor you started earlier,
and you can verify that by looking at the workers console output.

The basic Actor API expose three more methods for sending a message to a remote actor:
 * :py:meth:`~.actors.Actor.send` - sends to a particular actor instance
 * :py:meth:`~.actors.Actor.throw` - sends to an actor instance of the same type
 * :py:meth:`~.actors.Actor.scatter` - sends to all actor instances of the same type

The above methods can be invoked on an instance of :py:class:`~.actors.ActorProxy` class.

Calling a method with parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parameters to a method are passed as a dict.
here is an example how to call the method :py:meth:`~.examples.hello.GreeingActor.greet`
with an argument  :py:attr:`who`

.. code-block:: python

    actor.call('greet', {'who':'everyone'})

Getting a result back
~~~~~~~~~~~~~~~~~~~~~
Let's add another method to the :py:class:`GreetingActor` class
:py:meth:`how_are_you` that returns result.

.. code-block:: python

    from cell.actors import Actor

    class GreetingActor(Actor):
        class state:
            def greet(self, who='world'):
                return 'Hello %s' % who

            def how_are_you(self):
                return 'Fine!'


We can get the result in two steps:

* using a blocking call (set the nowait parameter to True), it blocks the executiomn until a result is delivered or a timeout is reached:

.. code-block:: python

    result = actor.call('greet', {'who':'everyone'}, nowait=True)


* using a non-blocking call (set the nowait parameter to True), it returns an isntanse of the :py:class:`~.AsyncResult` class.

.. code-block:: python

    result = actor.call('greet', {'who':'everyone'}, nowait=False)



Stopping an actor
~~~~~~~~~~~~~~~~~~

You can manually stop an actor using its id.

.. code-block:: python

    actors_mng.stop_actor_by_id(actor.id)

Where to go from here
~~~~~~~~~~~~~~~~~~~~~
If you want to learn more you should explore the examples in the :py:mod:`examples` module in the cell codebase
and study the :ref:`User Guide <guide>`.