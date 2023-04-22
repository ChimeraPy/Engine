=================
API Reference
=================

DataChunk
------------

.. autoclass:: chimerapy.DataChunk
   :members: __init__, add, get, update
   :member-order: bysource

Node
------------

.. autoclass:: chimerapy.Node
   :members: __init__, setup, step, teardown, main
   :member-order: bysource

Graph
------------

.. autoclass:: chimerapy.Graph
   :members: is_valid, plot
   :member-order: bysource

Worker
------------

.. autoclass:: chimerapy.Worker
   :members: __init__, connect, shutdown
   :member-order: bysource

Manager
------------

.. autoclass:: chimerapy.Manager
   :members: __init__, commit_graph, step, start, stop, shutdown
   :member-order: bysource
