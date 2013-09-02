
Hello World
===================
Here we will demonstrate the main cell features starting with a simple Hello World example.
To understand what is going on, we will break the example into pieces.

.. contents::
    :local:


Defining an actor
~~~~~~~~~~~~~~~~~

Actors are implemented by extending the :py:class:`~.actors.Actor` class and then defining a series of supported methods.
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
Cell targets distributed actors management. Therefore, creating an actor means spawning an actor consumer
remotely by sending a command to a celery worker agent. That is why before spawning an actor, you need to have a celery worker running:

.. code-block:: bash

    $ celery worker

Actors are created via :py:meth:`~.agents.dAgent.spawn` method, called on an instance of :py:class:`~.agents.dAgent` (distributed agent).
Agents are components responsible for creating and stopping actors on celery workers.
Each celery worker has embedded agent component :py:class:`dAgent` (distributed agent) listening for commands.
:py:meth:`~.agents.dAgent.spawn` is invoked with a class of type :py:class:`Actor` or its derivative
and starts an actor of that type remotely (in the celery worker).
The method returns a proxy (an instance of :py:class:`~.actors.ActorProxy`) for the remotely started actor.
The proxy holds the unique identifier, which ensures the messages can be delivered unambiguously to the same remote actor.

The code below starts a :py:class:`GreetingActor` and then invokes its py:meth:`greeting` method.

.. code-block:: python

    from cell.agents import dAgent
    from kombu import Connection

    connection = Connection()
    # STEP 1: Create an agent
    agent = dAgent(connection)
    # STEP 2: Pass the actor type to spawn method
    greeter = agent.spawn(GreetingActor)

    # STEP 3: Use actor proxy to call methods on the remote actor
    greeter.send('greet')


Calling a method
~~~~~~~~~~~~~~~~~
The cell actor model comes with few build-in delivery policies.
Here we demonstrate direct delivery - the message is send to a particular actor instance.
(Look at the end of the section for the other delivery options.)

.. code-block:: python

    actor.send('greet')

The :py:meth:`greet` method has been executed and you can verify that by looking at the workers console output.
The remote actor of type GreetingActor you created earlier handle the method.
The message 'Hello world' should be printed on the celery worker console.
Note that the call is asynchronous and since 'great' is a void method no result will be returned.
For getting results back and invoking methods synchronously check :ref:`Getting a result back section`

The basic Actor API expose three more methods for sending a message:
 * :py:meth:`~.actors.Actor.send` - sends to a particular actor instance
 * :py:meth:`~.actors.Actor.throw` - sends to an actor instance of the same type
 * :py:meth:`~.actors.Actor.scatter` - sends to all actor instances of the same type

For more information on the above options, see the :ref:`Delivery options` section

Calling a method with parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Here is an example how to call the method :py:meth:`~.examples.hello.GreeingActor.greet`
with an argument :py:attr:`who`

.. code-block:: python

    actor.send('greet', {'who':'everyone'})

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

* using a **blocking call** (set the nowait parameter to True), it blocks the execution until a result is delivered or a timeout is reached:

.. code-block:: python

    result = actor.send('greet', {'who':'everyone'}, nowait=True)

d.. warning:: If you are using blocking calls, greenlets should be enabled in the celery worker:

Greenlets can be enabled either by using eventlet or using gevent:

.. code-block:: python

    >>> celery worker -P eventlet -c 100

or

.. code-block:: python

    >>> celery worker -P gevent -c 100

You can read more about concurrency in celery in `here`_

.. _`here`: http://docs.celeryproject.org/en/latest/userguide/concurrency/index.html

* using a **non-blocking call** (set the nowait parameter to False, the default), it returns an an :py:class:`~.AsyncResult` instance.
:py:class:`~.AsyncResult` can be used to check the state of the result, get the return value or if the method failed, the exception and traceback).

.. code-block:: python

    result = actor.send('greet', {'who':'everyone'}, nowait=False)

The :meth:`~@AsyncResult.result` returns the result if it is ready or wait for the result to complete

.. code-block:: python

    result = actor.send('greet', {'who':'everyone'}, nowait=False)
    print result.result

See cell.result for the complete result object reference.

Stopping an actor
~~~~~~~~~~~~~~~~~~
We can stop an actor if we know its id.

.. code-block:: python

    agent.kill(actor.id)

:py:meth:`agents.dAgent.kill` is a broadcast command sent to all agents.
If an agents doesn't have in its registry the given actor.id, it will dismiss the command,
otherwise it will gently kill the actor and delete it from its registry.

Where to go from here
~~~~~~~~~~~~~~~~~~~~~
If you want to learn more you should explore the examples in the :py:mod:`examples` module in the cell codebase
and/or study the :ref:`User Guide <guide>`.