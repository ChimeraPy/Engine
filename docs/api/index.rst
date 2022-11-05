=================
API Reference
=================

Node
------------

.. autoclass:: chimerapy.Node
   :members: __init__, prep, step, teardown

Worker
------------

.. autoclass:: chimerapy.Worker
   :members: __init__, connect, step, shutdown
   :undoc-members: __init__, connect, step, shutdown

Manager
------------

.. autoclass:: chimerapy.Manager
   :members: __init__, register_graph, map_graph, commit_graph, step, start, stop, shutdown
   :undoc-members: __init__, register_graph, map_graph, commit_graph, step, start, stop, shutdown


Graph
------------

.. autoclass:: chimerapy.Graph
   :undoc-members:
   :members:
