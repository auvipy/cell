#############################################
 cell - Actor framework
#############################################

:Version: 0.0.3

Synopsis
========

`cell` is an actor framework for `Kombu`_ and `celery`_ .

.. _`Kombu`: http://pypi.python.org/pypi/kombu
.. _`celery`: http://pypi.python.org/pypi/celery


What is an Actor
================

The actor model was first proposed by Carl Hewitt in 1973 `[1]`_ and was improved, among others,
by Gul Agha `[2]`_.

.. _`[1]`: http://dl.acm.org/citation.cfm?id=1624804
.. _`[2]`: http://dl.acm.org/citation.cfm?id=7929

An Actor is an entity (a class in cell), that has a mailbox and a behaviour. Actors communicate between each other only by exchanging messages.
Upon receiving a message, the behaviour of the actor is executed, upon which the actor can send a number of messages to other actors,
create a number of actors or change its internal state.

Cell supports:

* distributed actors (actors are started on celery workers)
* Remoting: Communicating with actors running on other hosts
* Routers: it supports round-robin, direct and broadcast delivery of actor messages. You can create custom routers on top of it, implementing Actors as routers (joiner, collector, gatherer).

Why should I use it?
====================

In a nutshell:

* Horizontal scalability with actors across multiple nodes
* You get asynchronous message passing for free
* If you are already using celery, all comes for free, no additional setup is required
* Control over the tasks distribution
* More flexible configurations of nodes
* Well known abstraction
* Easy learning curve (check teh 30 sec video to get you started)

if you are a celery user:
-------------------------
* You can use Actors, instead of task-based classes:
(You can program with classes and not tasks)

* Stateful execution. You can link actors together and their execution, creating complex workflows.
You can control the execution per actor/not per worker.

* Better control over work distribution (You can target the same worker for a given task):

.. code-block:: python

    adder.send.add(2, 2)
    adder.send.add(2, 2)

.. If you are an actor frameworks user:
.. -----------------------------------

.. Cell supports different routers, distributed actors, supervision strategies (retry, exceptions).
.. Typing actors, workflows with actors and checks on the workflow.
.. Workflow in cell:
.. Distributed work:
.. Flexible configuration: (with actors you can implement routing behaviour that is needed)

If you are a general Pythonist
------------------------------
Having a framework for distributed actor management in your toolbox is a must, bacause:

* simplify the distributed processing of tasks.
* vertical scalability:
* Fair work distribution, load balancing, sticky routing

.. Features you will love:
.. ~~~~~~~~~~~~~~~~~~~~~~~
..
.. - exceptions happened during background processing are collected and can be browsed later
.. - workflows: run Task1, pass it's results to Task2, Task3 and Task4 which are run in parallel, collect their results and pass to Task5
.. - How do you handle tasks


Installation
============

You can install `cell` either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install cell

To install using `easy_install`,::

    $ easy_install cell

If you have downloaded a source tarball you can install it
by doing the following,::

    $ python setup.py build
    # python setup.py install # as root


Quick how-to
============
If you are too impatient to start, here are the 3 quick steps you need to run 'Hello, world!' in cell:
(You can also check the `Demo`_ video)

.. _`Demo`: http://www.doc.ic.ac.uk/~rn710/videos/FirstSteps.mp4

* Define an Actor

.. code-block:: python

    from cell.actors import Actor

    class GreetingActor(Actor):
        class state:
            def greet(self, who='world'):
                print 'Hello %s' % who



* Start celery with an amqp broker support

.. code-block:: python

    >>> celery worker -b 'pyamqp://guest@localhost'

* Invoke a method on an actor instance:
.. code-block:: python

    from cell.agents import dAgent
    from kombu import Connection
    from examples.greeting import GreetingActor

    connection = Connection('amqp://guest:guest@localhost:5672//')
    agent = dAgent(connection)
    greeter = agent.spawn(GreetingActor)
    greeter.call('greet')

The full source code of the example from :py:mod:`examples` module.
To understand what is going on check the :ref:`Getting started <getting-started>` section.


Getting Help
============

Mailing list
------------

Join the `carrot-users`_ mailing list.

.. _`carrot-users`: http://groups.google.com/group/carrot-users/

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at http://github.com/celery/cell/issues/

Contributing
============

Development of `cell` happens at Github: http://github.com/celery/cell

You are highly encouraged to participate in the development. If you don't
like Github (for some reason) you're welcome to send regular patches.

License
=======

This software is licensed under the `New BSD License`. See the `LICENSE`
file in the top distribution directory for the full license text.

Copyright
=========

Copyright (C) 2011-2013 GoPivotal, Inc.
