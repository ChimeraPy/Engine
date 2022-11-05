Defining Your Pipeline
######################

When constructing your pipeline, the use of :class:`Node<chimerapy.Node>` and :class:`Graph<chimerapy.Graph>` will largely contribute to the data flow and processing. This is done through the representation of a directed acyclic graph, meaning a graph with edges with a direction that do not make a cycle. If a cycle is detected, then ChimeraPy will not allow you to register the graph. Regardless, there are some visualization tools to help construct a graph.

Instantiate Nodes
*****************

To define you DAG, first create the nodes that will populate the DAG::

    import chimerapy as cp

    a = cp.Node(name="a")
    b = cp.Node(name="b")
    c = cp.Node(name="c")
    d = cp.Node(name="d")
    e = cp.Node(name="e")
    f = cp.Node(name="f")

For now we are just going to use empty :class:`Node<chimerapy.Node>` for illustrative purposes, but in your own implementation, these nodes would include sensor collecting, processing, and feedback.

Add Nodes to Graph
******************

After creating the :class:`Nodes<chimerapy.Node>`, we add them to the :class:`Graph<chimerapy.Graph>`, like this::

    graph = cp.Graph()
    graph.add_nodes_from([a,b,c,d,e,f])
    graph.add_edges_from([[a, b],[c, d], [c, e], [b, e], [d,f], [e,f]])

Check Graph
***********

With the graph configuration complete, we can visually confirm that the DAG is constructed correctly, by using the following command::

    # Should create a matplotlib figure
    graph.plot()

This creates the following plot:

.. image:: ../_static/examples/example_pipeline.png
  :width: 90%
  :alt: Example Pipeline
