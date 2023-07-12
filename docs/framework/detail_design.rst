Details on the System's Design
##############################

.. image:: ../_static/architecture/DetailedSystem.png
  :width: 90%
  :alt: System's Detailed Design

By stripping the hood of ChimeraPy and see its inter-workings, it comes down to a combination of AIOHTTP, ZeroMQ, and parallel programming. The communication within ChimeraPy can be broken down into two categories: HTTP/WebSocket and PUB/SUB. The :class:`Manager<chimerapy.engine.Manager>` and :class:`Worker<chimerapy.engine.Worker>` send messages via HTTP and WebSockets, including the :class:`Server<chimerapy.engine.networking.Server>` and :class:`Client<chimerapy.engine.networking.Client>`. These messages are not the raw data streams, instead they are operational instructions and information that orchestrates the clusters execution. The data streams use a different communication protocol and channels. The communication between :class:`Node<chimerapy.engine.Node>` uses ZeroMQ's PUB/SUB pattern, optimized for speed, robustness, and latency. In specific, the node-communication uses the :class:`Publisher<chimerapy.engine.networking.Publisher>` and :class:`Subscriber<chimerapy.engine.networking.Subscriber>` implementations.

Multiprocessing is at the heart of ChimeraPy, as the base class :class:`Node<chimerapy.engine.Node>` is a subclass of Python's build-in multiprocessing's :class:`Process<multiprocessing.Process>`. Each :class:`Node<chimerapy.engine.Node>` executes its ``setup``, ``step``, and ``teardown`` within its own process, to reduce CPU bound limitations.

In the other side of the parallel programming spectrum, multithreading and AsyncIO are used for relieve the IO bound. More specifically, multithreading is used in active working while waiting, such as writing to video to memory, while AsyncIO is used for networking.

More details in how each component works can be found in the :ref:`Developer's Documentation<developerdocs>`.
