=================
API Reference
=================

Node
------------

.. autoclass:: chimerapy.Node
   :members: __init__, prep, step, teardown, main
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
