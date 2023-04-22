Basics
######

.. _basics:

For the basics, we start with the smallest component of the system, the :class:`Node<chimerapy.Node>`. The :class:`Node<chimerapy.Node>` provides the container for logic in how to collect, process, and return data within the ChimeraPy framework. Then, we will discuss how to incorporate a :class:`Node<chimerapy.Node>` into the directed acyclic graph (DAG) pipeline through the :class:`Graph<chimerapy.Graph>`. We will finish the setup with configuring our local or distributed cluster with :class:`Manager<chimerapy.Manager>` and :class:`Worker<chimerapy.Worker>`. Once all setup is complete, we can execut the system and control the cluster.

Creating a Custom Node
**********************

To create a custom :class:`Node<chimerapy.Node>`, we overwrite the ``setup``, ``step``, and ``teardown`` methods. Below is an example of a :class:`Node<chimerapy.Node>` that generates a sequence of random numbers::

    import chimerapy as cp
    import numpy as np

    class RandomNode(cp.Node):

        def setup(self):
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

For this example, the ``RandomNode`` is a source node. Step and sink have a ``step(self, data: Dict[str, DataChunk])`` method to retrieve input datas. Since source :class:`Node<chimerapy.Node>` do not have inputs, it has a simplified ``step(self)`` method instead.

The DataChunk Container
***********************

A main attribute of ChimeraPy is that it doesn't assume what type of data is being transmitted between :class:`Nodes<chimerapy.Node>`. Therefore, when developing your custom node implementations, the ``step`` function can return anything that is serializable. There are moments when this isn't beneficial. For example, to make video streaming work in real time, it is required to compress video frames with an algorithm optimized for images. This implies that ChimeraPy must then know what is being transmitted. This is achieved through the use of the :class:`DataChunk<chimerapy.DataChunk>` container. This is an example for a video streaming Node::


    class ScreenCapture(cp.Node):

        def step(self) -> cp.DataChunk:

            time.sleep(1 / 10)
            frame = cv2.cvtColor(
                np.array(ImageGrab.grab(), dtype=np.uint8), cv2.COLOR_RGB2BGR
            )

            # Create container and send it
            data_chunk = cp.DataChunk()
            data_chunk.add("frame", frame, "image")
            return data_chunk

As of now, the only special compression algorithms incorporated into ChimeraPy are for images. When transmitting images, use the DataChunk with the ``image`` content-type option. Otherwise, ChimeraPy will mark the Node's output as ``other`` and will apply a generic serialization and compression method.

Creating a DAG
**************

The creation of a DAG is done through the :class:`Graph<chimerapy.Graph>` class. The :class:`Graph<chimerapy.Graph>` is a subclass of `NetworkX <https://networkx.org>`_ `DiGraph <https://networkx.org/documentation/stable/reference/classes/digraph.html>`_. To start, we create the Nodes we are interested in putting into our DAG and then, by re-using `nx.DiGraph` API, we can add nodes and edges. An example is shown below::

    import chimerapy as cp

    class SourceNode(cp.Node):
        def setup(self):
            self.value = 2

        def step(self):
            time.sleep(0.5)
            return self.value

    class StepNode(cp.Node):
        def setup(self):
            self.coef = 3

        def step(self, data: Dict[str, cp.DataChunk]):
            time.sleep(0.1)
            return self.coef * data["Gen1"].get('default')['value']

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

For a local cluster, we can create the :class:`Worker<chimerapy.Worker>` instance within the local machine. This is how it works::

    import chimerapy as cp

    # Create local objects
    manager = cp.Manager() # using default configs
    worker = cp.Worker(name="local") # creating local worker

    # Connect
    worker.connect(host=manager.host, port=manager.port)

For a distributed cluster, the connection setup requires more work. First, we start the :class:`Manager<chimerapy.Manager>` in the main computer::

    $ python
    >>> import chimerapy as cp
    >>> manager = cp.Manager()
    2022-11-03 22:37:55 [INFO] chimerapy: Server started at Port 9000

Once the :class:`Manager<chimerapy.Manager>` started, the next step is to access the worker computers and use the ChimeraPy :class:`Worker<chimerapy.Worker>` connect entrypoint to establish the connection. With the following command, we can connect the worker computer::

    $ # You will have to obtain your Manager's IP address (ifconfig)
    $ cp-worker --port 10.0.0.153 --port 9000 --name remote

With the correct networking information (change ``10.0.0.153`` with the ip address of your computer hosting the :class:`Manager<chimerapy.Manager>`, the :class:`Worker<chimerapy.Worker>` should connect and the :class:`Manager<chimerapy.Manager>` should report the :class:`Worker<chimerapy.Worker>` as registered::

    2022-11-03 22:42:05 [INFO] chimerapy: <Server Manager MANAGER_MESSAGE->WORKER_MESSAGE>: Got connection from ('10.0.0.171', 44326)

This message informs us that the :class:`Worker<chimerapy.Worker>` connected successfully.

Worker-Node Mapping
===================

After setting up our cluster, we need to delegate :class:`Nodes<chimerapy.Node>` to the :class:`Workers<chimerapy.Worker>`. This is achieved by using a :class:`Graph<chimerapy.Graph>` object and a mapping between the workers and the nodes. Then, through a dictionary mapping, where the keys are the workers' names and the values are list of the nodes' names, we can specify which workers will perform which node tasks. Here is an example::

    # Then register graph to Manager
    manager.commit_graph(
        graph=graph,
        mapping={
            "local": ["source", "step"],
        }
    )

We then commit the :class:`Graph<chimerapy.Graph>` to the :class:`Worker<chimerapy.Worker>`. All the :class:`Nodes'<chimerapy.Node>` code are located within the :class:`Manager's<chimerapy.Manager>` computer; therefore, these compartmentalized code needs to be sent to the :class:`Workers<chimerapy.Worker>`. The ``commit_graph`` routine can take some time based on the number of :class:`Worker<chimerapy.Worker>`, :class:`Nodes<chimerapy.Node>`, and their code size hence waiting until all nodes are ready.

Execution
*********

Now we are ready to execute the system, this is achieved through the :class:`Manager<chimerapy.Manager>`'s control API. Below shows how to start, execute for 10 seconds, and then stop the system::

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(10)
    manager.stop()
