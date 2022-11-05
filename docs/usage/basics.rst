Basics
######

.. _basics:

For the basics, we start with the smallest component of the system, the :class:`Node<chimerapy.Node>`. The :class:`Node<chimerapy.Node>` provides the container for logic in how to collect, process, and return data within the ChimeraPy framework. Then, we will discuss how to incorporate a :class:`Node<chimerapy.Node>` into the directed acyclic graph (DAG) pipeline through the :class:`Graph<chimerapy.Graph>`. We will finish the setup with configuring our local or distributed cluster with :class:`Manager<chimerapy.Manager>` and :class:`Worker<chimerapy.Worker>`. Once all setup is complete, we can execut the system and control the cluster.

Creating a Custom Node
**********************

To create a custom :class:`Node<chimerapy.Node>`, we overwrite the ``prep``, ``step``, and ``teardown`` methods. Below is an example of a :class:`Node<chimerapy.Node>` that generates a sequence of random numbers::

    import chimerapy as cp
    import numpy as np

    class RandomNode(cp.Node):

        def prep(self):
            # Create the generator to be used later
            self.rng = np.random.default_rng(0)

        def step(self):
            # Set the execution to 1 every second
            time.sleep(1)
            return self.rng.random()

        def teardown(self):
            # Not necessary here, but can be used to close
            # files or disconnect from devices.
            del self.rng

It is important to remember that there is three possible node types: source, step, and sink. This is the taxonomy:

* Source: no inputs, yes outputs
* Step: yes inputs, yes outputs
* Sink: yes inputs, no outputs

For this example, the ``RandomNode`` is a source node. Step and sink have a ``step(self, data: Dict[str, Any])`` method to retrieve input datas. Since source :class:`Node<chimerapy.Node>` do not have inputs, it has a simplified ``step(self)`` method instead.

Creating a DAG
**************

The creation of a DAG is done through the :class:`Graph<chimerapy.Graph>` class. The :class:`Graph<chimerapy.Graph>` is a subclass of `NetworkX <https://networkx.org>`_ `DiGraph <https://networkx.org/documentation/stable/reference/classes/digraph.html>`_. To start, we create the Nodes we are interested in putting into our DAG and then, by re-using `nx.DiGraph` API, we can add nodes and edges. An example is shown below:::

    import chimerapy as cp

    class SourceNode(cp.Node):
        def prep(self):
            self.value = 2

        def step(self):
            time.sleep(0.5)
            return self.value

    class StepNode(cp.Node):
        def prep(self):
            self.coef = 3

        def step(self, data: Dict[str, Any]):
            time.sleep(0.1)
            return self.coef * data["Gen1"]

    if __name__ == "__main__":

        # First, create the Nodes
        source_node = SourceNode(name="source")
        step_node = StepNode(name="step")

        # Create the graph
        graph = cp.Graph()

        # Then add the nodes to the graph
        graph.add_nodes_from([source_node, step_node])
        graph.add_edge(source_node, step_node)

.. note::
   When creating a Node instance, it requires a name to be provided.

Now with the creation of our graph, we can setup our local or distributed cluster.

Cluster Setup
*************

During our cluster setup, we have the many options and configurations to establish. These include what :class:`Worker<chimerapy.Worker>` objects we want to connect, if we are using a local or distributed system, and delegating :class:`Node<chimerapy.Node>` objects to :class:`Worker<chimerapy.Worker>`.

Manager-Worker Connection
=========================

For a local cluster, we can create the :class:`Worker<chimerapy.Worker>` instance within the local machine. This is how it works:::

    import chimerapy as cp

    # Create local objects
    manager = cp.Manager() # using default configs
    worker = cp.Worker(name="local") # creating local worker

    # Connect
    worker.connect(host=manager.host, port=manager.port)

For a distributed cluster, the connection setup requires more work. First, we start the :class:`Manager<chimerapy.Manager>` in the main computer.::

    $ python
    >>> import chimerapy as cp
    >>> manager = cp.Manager()
    2022-11-03 22:37:55 [INFO] chimerapy: Server started at Port 9000

Once the :class:`Manager<chimerapy.Manager>` started, the next step is to access the worker computers and use the ChimeraPy :class:`Worker<chimerapy.Worker>` connect entrypoint to establish the connection. With the following command, we can connect the worker computer:::

    $ # You will have to obtain your Manager's IP address (ifconfig)
    $ cp-worker --port 10.0.0.153 --port 9000 --name remote

With the correct networking information (change ``10.0.0.153`` with the ip address of your computer hosting the :class:`Manager<chimerapy.Manager>`, the :class:`Worker<chimerapy.Worker>` should connect and the :class:`Manager<chimerapy.Manager>` should report the :class:`Worker<chimerapy.Worker>` as registered:::

    2022-11-03 22:42:05 [INFO] chimerapy: <Server Manager MANAGER_MESSAGE->WORKER_MESSAGE>: Got connection from ('10.0.0.171', 44326)

This message informs us that the :class:`Worker<chimerapy.Worker>` connected successfully.

Worker-Node Mapping
===================

After setting up our cluster, we need to delegate :class:`Nodes<chimerapy.Node>` to the :class:`Workers<chimerapy.Worker>`. First, you register the :class:`Graph<chimerapy.Graph>` to the :class:`Worker<chimerapy.Worker>` to verify a valid graph. Then, through a dictionary mapping, where the keys are the workers' names and the values are list of the nodes' names, we can specify which workers will perform which node tasks. Here is an example:::

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph(
        {
            "local": ["source", "step"],
        }
    )

    # Commiting the graph by sending it to the workers
    manager.commit_graph()
    manager.wait_until_all_nodes_ready(timeout=10)

We then commit the :class:`Graph<chimerapy.Graph>` to the :class:`Worker<chimerapy.Worker>`. All the :class:`Nodes'<chimerapy.Node>` code are located within the :class:`Manager's<chimerapy.Manager>` computer; therefore, these compartmentalized code needs to be sent to the :class:`Workers<chimerapy.Worker>`. The ``commit_graph`` routine can take some time based on the number of :class:`Worker<chimerapy.Worker>`, :class:`Nodes<chimerapy.Node>`, and their code size hence waiting until all nodes are ready.

Execution
*********

Now we are ready to execute the system, this is achieved through the :class:`Manager<chimerapy.Manager>`'s control API. Below shows how to start, execute for 10 seconds, and then stop the system::

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(10)
    manager.stop()
