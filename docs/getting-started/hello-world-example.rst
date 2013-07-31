
Hello World
===================
Here we will demonstrate the main cell features starting with a simple Hello World example.

.. contents::
    :local:

To understand what is going on, we will break the example into pieces.

Defining an actor
~~~~~~~~~~~~~~~~~

Actors are implemented by extending the :py:class:`Actor` class and then defining a series of supported methods.
The supported methods should be encapsulated in the Actor's internal :py:class:`~.Actor.state` class.
Our first actor implements one method called greet.

.. code-block:: python

    from cell.actors import Actor

    class GreetingActor(Actor):
        class state:
            def greet(self, who='world'):
                print 'Hello %s' % who

Starting an actor
~~~~~~~~~~~~~~~~~~~~~~~
Distributed actors are managed via internal components( an instance of :py:class:`dAgent`) in celery.
Therefore, you need to have the the celery worker running to spawn an actor remotely.

Agents are components responsible for creating and stopping actors on celery workers.
Each celery worker has embedded agent component :py:class:`dAgent` (distributed agent) listening for commands.
In the current version cell supports only distributed actors. Therefore, creating an actor means spawning an actor consumer
remotely by sending a command to a celery worker agent. Thereofer, before spawning an actor, you need to have a celery worker running:

.. code-block:: bash

    $ celery worker

Actors are created via :py:meth:`agent.spawn`, called on an instance of :py:class:`dAgent`.
:py:meth:`agent.spawn` spawn accepts class of type :py:class:`Actor` or its derivative and returns an :py:class:`ActorProxy` instance,
holding the unique id of the actor started on the worker. This way, the messages can be delivered unambiguously to the remote actor.

.. code-block:: python

    from cell.agents import dAgent
    from kombu import Connection

    connection = Connection()
    # STEP 1: Create an agent
    agent = dAgent(connection)
    # STEP 2: Pass the actor type to spawn method
    greeter = agent.spawn(GreetingActor)

    # STEP 3: Use actor proxy to call methods on the remote actor
    greeter.call('greet')


Calling a method
~~~~~~~~~~~~~~~~~
There are few delivery policies, implemented by the cell actor model.
Here we will demonstrate direct delivery. The message is send to a particular actor instance.

.. code-block:: python

    actor.call('greet')

The :py:meth:`greet` has been executed and you can verify that by looking at the workers console output.
The remote actor you created earlier will handle the method call.
The call is asynchronous. Since we are invoking a void method no result will be returns.
For getting results from an actor call, check :ref:`Getting a result back section`


The basic Actor API expose three more methods for sending a message to a remote actor:
 * :py:meth:`~.actors.Actor.send` - sends to a particular actor instance
 * :py:meth:`~.actors.Actor.throw` - sends to an actor instance of the same type
 * :py:meth:`~.actors.Actor.scatter` - sends to all actor instances of the same type

For more information, see the :ref:`Delivery options` section

Calling a method with parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can pass additional parameters to a method using a dictionary.
Here is an example how to call the method :py:meth:`~.examples.hello.GreeingActor.greet`
with an argument :py:attr:`who`

.. code-block:: python

    actor.call('greet', {'who':'everyone'})

Getting a result back
~~~~~~~~~~~~~~~~~~~~~
Let's add another method to the :py:class:`GreetingActor` class
:py:meth:`how_are_you` that returns a result.

.. code-block:: python

    from cell.actors import Actor

    class GreetingActor(Actor):
        class state:
            def greet(self, who='world'):
                print 'Hello %s' % who

            def how_are_you(self):
                return 'Fine!'


We can get the result in two ways:

* using a blocking call (set the nowait parameter to True), it blocks the execution until a result is delivered or a timeout is reached:

.. code-block:: python

    result = actor.call('greet', {'who':'everyone'}, nowait=True)


* using a non-blocking call (set the nowait parameter to False), it returns an an :py:class:`~.AsyncResult` instance.

which can be used to check the state of the result,
or get the return value (or if the method failed, the exception and traceback).

.. code-block:: python

    result = actor.call('greet', {'who':'everyone'}, nowait=False)

The :meth:`~@AsyncResult.result` returns the result if it is ready or wait for the result to complete

.. code-block:: python

        result = actor.call('greet', {'who':'everyone'}, nowait=False)

See cell.result for the complete result object reference.

Stopping an actor
~~~~~~~~~~~~~~~~~~
Actors are stopped by sending a remote command to the agent component.
To stop an actor, we should know its id.

.. code-block:: python

    agent.stop_actor_by_id(actor.id)

Where to go from here
~~~~~~~~~~~~~~~~~~~~~
If you want to learn more you should explore the examples in the :py:mod:`examples` module in the cell codebase
and/or study the :ref:`User Guide <guide>`.