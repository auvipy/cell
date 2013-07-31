.. _guide:

User Guide
============

“We are all merely Actors” Ask Solem

.. contents::
    :local:
    :depth: 1

Spawning an actor
~~~~~~~~~~~~~~~~~~~~~


Spawning an actor with arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fun fact: Agents are actors as well.

Actor Delivery Types
~~~~~~~~~~~~~~~~~~~~

Let's create two actors to use throughout the section.

.. code-block:: python

    logger1 = agent.spawn(Logger)
    logger2 = agent.spawn(Logger)

logger1 and logger2 are ActorProxies for the actors started remotely as a result of the spawn command.
The actors are of the same type (Logger), but have different identifiers.

Cell supports few sending primitives, implementing the different delivery policies:

* direct (using :py:meth:`~.actor.Actor.call`) - sends a message to a concrete actor (to the invoker)
.. code-block:: python

    logger1.call('log', {'msg':'the quick brown fox ...'})

The message is delivered to the remote counterpart of logger1.

* round-robin (using :py:meth:`~.actor.Actor.throw`) - sends a message to an arbitrary actor with the same actor type as teh invoker

.. code-block:: python

    logger1.throw('log', {'msg':'the quick brown fox ...'})

The message is delivered to the remote counterparts of either logger1, or logger2.

* broadcast (using :py:meth:`~.actor.Actor.scatter`) - sends a message to all running actors, having the same actor type as te invoker

.. code-block:: python

    logger1.scatter('log', {'msg':'the quick brown fox ...'})

The message is delivered to both logger 1 and logger 2.
All running actors, having a type Logger will receive and process the message.

The picture below sketches the three delivery options for actors. It shows what each primitive does and how each delivery option
is implemented in terms of transport entities (exchanges and queues). Each primitive (depending on its type and the type of the actor)
has an exchange and a routing key to use when sending.

.. image:: delivery-options.*

Wrapping your method calls
~~~~~~~~~~~~~~~~~~~~~~~~~~
Passing actor methods as a string is often not a convenient option.
We can easily refactor the Logger class to expose its state methods in more convenient way.

.. code-block:: python

    class Logger(Actor):
        class state:
            def log(msg):
                print msg

        def log(msg):
            self.call('log', {'msg':msg})

Request-reply pattern (waiting for the result of another actor)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: When using actors, always start celery with greenlet support enabled!
(see `Greenlets in celery`_ for more information)

Depending on your environment and requirements, you can start green workers in one of these ways:

.. code-block:: bash
    $ celery worker -P eventlet -c 1000

 or

.. code-block:: bash
    $ celery -P gevent -c 1000

.. _`Greenlets in celery`_ http://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html

When greenlet is enbaled, each method is executed in its own greenlet.

Actor model prescribes that an actor should not block and wait for the result of another actor. Therefore, the result should always be passed
via callback. However, if you are not a fen of the CPS (continuation passing style) and want to preserve your control flow, you can use greenlets.

Below, the two options (callbacks and greenlets) are explained in more details:

* via greenlets

.. warning:: To use this option, GREENLETS SHOULD BE ENABLED IN THE CELERY WORKERS running the actors. If not, a deadlock is possible.


(Should be rewritten)
If workers are not green, this implementation will result in a deadlock.
Consider the following example that defines two actors  - Incrementer and Counter.
To count to a given target, the Counter calls the Incrementer inc method  in a loop,
asking for the next value. Therefore, the Counter waits for the result of the Incrementer every time.
If both teh Incrementer and the Counter are created on the same worker and this worker does not support greenlest.
the Counter worker will be blocked, waiting for the result of the Incrementer, preventing the Incrementer
from receiving commands and therefore causing a dealock.
However, if the worker supports greenlets, only the Counter greenlet will block, allowing the worker execution flow to continue.


.. code-block:: python

    class Incrementer(Actor):
        class state:
            def inc(self, n)
                return n + 1

        def inc(self, n):
            self.call('inc', {'n':n}, nowait=False)

    class Counter(Actor):
        class state:
            def count_to(self, target)
                incrementer = self.agent.spawn(Incrementer)
                next = 0
                while target:
                    print next
                    next = incrementer.inc(next)
                    target -= 1

* via callbacks


Scheduling Actors in celery
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* when greenlets are enabled

Every actor message that is received is process in a separate greenlet,

* when greenlets are disabled

All messages are processed by the same thread and processed in order of delivery.
Thus, it is up to the broker in what order the messages will be delivered and processed.


Emit method or how to bind actors together
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In addition to the inboxes (all exchanges, explained in the :ref:`Actor Delivery Types`), each actor also have an outbox.
Outboxes are used when we  want to bind actors together. An example is forwarding messages from one actor to another.
This means that the original sender address/reference is maintained even
though the message is going through a 'mediator'.
This can be useful when writing actors that work as routers, load-balancers, replicators etc.
The :py:meth:`~.cell.actors.Actor.emit` method explicitly send a message to its actor outbox.
The binding can be added and removed dynamically when needed by the application.

.. code-block:: python

    logger1 = agent.spawn(Logger)
    logger2 = agent.spawn(Logger)

    logger1 |forward| logger2
    logger1.emit('log', {'msg':'I will be printed from logger2'})
    logger1 |stop_forward| logger2
    logger1.emit('log', {'msg':'I will be printed from logger1'})



Receiving messages
~~~~~~~~~~~~~~~~~~
An actor message has a name (label) and arguments.
Each message name should have a corresponding method in the Actor's internal state class.
To handle a generic message files 
Otherwise, an error code is returned as a result of the message call.
However, if fire and forget called is used (call with nowait argument set to True),
no error code will be returned. You find the error expecting the worker log or the the worker console.

An actor handle generic

Receiving timeout
~~~~~~~~~~~~~~~~~~
