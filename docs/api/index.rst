=================
API Reference
=================

Node
------------

.. autoclass:: chimerapy.Node
   :members: __init__, prep, step, teardown

Manager
------------

.. autoclass:: chimerapy.Manager
   :members: __init__, register_graph, map_graph, commit_graph, step, start, stop, shutdown
   :undoc-members: __init__, register_graph, map_graph, commit_graph, step, start, stop, shutdown

Worker
------------

.. autoclass:: chimerapy.Worker
   :members: __init__, connect, step, shutdown
   :undoc-members: __init__, connect, step, shutdown

Graph
------------

.. autoclass:: chimerapy.Graph
   :undoc-members:
   :members:

