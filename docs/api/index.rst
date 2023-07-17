=================
API Reference
=================

DataChunk
------------

.. autoclass:: chimerapy.engine.DataChunk
   :members: __init__, add, get, update
   :member-order: bysource

Node
------------

.. autoclass:: chimerapy.engine.Node
   :members: __init__, setup, step, teardown, main
   :member-order: bysource

Graph
------------

.. autoclass:: chimerapy.engine.Graph
   :members: is_valid, plot
   :member-order: bysource

Worker
------------

.. autoclass:: chimerapy.engine.Worker
   :members: __init__, connect, shutdown
   :member-order: bysource

Manager
------------

.. autoclass:: chimerapy.engine.Manager
   :members: __init__, commit_graph, step, start, stop, shutdown
   :member-order: bysource
