Basics
######

.. _basics:

For the basics, we start with the smallest component of the system, the :class:`Node<chimerapy.engine.Node>`. The :class:`Node<chimerapy.engine.Node>` provides the container for logic in how to collect, process, and return data within the ChimeraPy framework. Then, we will discuss how to incorporate a :class:`Node<chimerapy.engine.Node>` into the directed acyclic graph (DAG) pipeline through the :class:`Graph<chimerapy.engine.Graph>`. We will finish the setup with configuring our local or distributed cluster with :class:`Manager<chimerapy.engine.Manager>` and :class:`Worker<chimerapy.engine.Worker>`. Once all setup is complete, we can execute the system and control the cluster.

Creating a Custom Node
**********************

To create a custom :class:`Node<chimerapy.engine.Node>`, we overwrite the ``setup``, ``step``, and ``teardown`` methods. Below is an example of a :class:`Node<chimerapy.engine.Node>` that generates a sequence of random numbers::

    import chimerapy.engine as cpe
    import numpy as np

    class RandomNode(cpe.Node):

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

For this example, the ``RandomNode`` is a source node. Step and sink have a ``step(self, data: Dict[str, DataChunk])`` method to retrieve input datas. Since source :class:`Node<chimerapy.engine.Node>` do not have inputs, it has a simplified ``step(self)`` method instead.

The DataChunk Container
***********************

A main attribute of ChimeraPy is that it doesn't assume what type of data is being transmitted between :class:`Nodes<chimerapy.engine.Node>`. Therefore, when developing your custom node implementations, the ``step`` function can return anything that is serializable. There are moments when this isn't beneficial. For example, to make video streaming work in real time, it is required to compress video frames with an algorithm optimized for images. This implies that ChimeraPy must then know what is being transmitted. This is achieved through the use of the :class:`DataChunk<chimerapy.engine.DataChunk>` container. This is an example for a video streaming Node::


    class ScreenCapture(cpe.Node):

        def step(self) -> cpe.DataChunk:

            time.sleep(1 / 10)
            frame = cv2.cvtColor(
                np.array(ImageGrab.grab(), dtype=np.uint8), cv2.COLOR_RGB2BGR
            )

            # Create container and send it
            data_chunk = cpe.DataChunk()
            data_chunk.add("frame", frame, "image")
            return data_chunk

As of now, the only special compression algorithms incorporated into ChimeraPy are for images. When transmitting images, use the DataChunk with the ``image`` content-type option. Otherwise, ChimeraPy will mark the Node's output as ``other`` and will apply a generic serialization and compression method.

Creating a DAG
**************

The creation of a DAG is done through the :class:`Graph<chimerapy.engine.Graph>` class. The :class:`Graph<chimerapy.engine.Graph>` is a subclass of `NetworkX <https://networkx.org>`_ `DiGraph <https://networkx.org/documentation/stable/reference/classes/digraph.html>`_. To start, we create the Nodes we are interested in putting into our DAG and then, by re-using `nx.DiGraph` API, we can add nodes and edges. An example is shown below::

    import chimerapy as cpe

    class SourceNode(cpe.Node):
        def setup(self):
            self.value = 2

        def step(self):
            time.sleep(0.5)
            return self.value

    class StepNode(cpe.Node):
        def setup(self):
            self.coef = 3

        def step(self, data: Dict[str, cpe.DataChunk]):
            time.sleep(0.1)
            return self.coef * data["Gen1"].get('default')['value']

    if __name__ == "__main__":

        # First, create the Nodes
        source_node = SourceNode(name="source")
        step_node = StepNode(name="step")

        # Create the graph
        graph = cpe.Graph()

        # Then add the nodes to the graph
        graph.add_nodes_from([source_node, step_node])
        graph.add_edge(source_node, step_node)

.. note::
   When creating a Node instance, it requires a name to be provided.

Now with the creation of our graph, we can setup our local or distributed cluster.

Cluster Setup
*************

During our cluster setup, we have the many options and configurations to establish. These include what :class:`Worker<chimerapy.engine.Worker>` objects we want to connect, if we are using a local or distributed system, and delegating :class:`Node<chimerapy.engine.Node>` objects to :class:`Worker<chimerapy.engine.Worker>`.

Manager-Worker Connection
=========================

For a local cluster, we can create the :class:`Worker<chimerapy.engine.Worker>` instance within the local machine. This is how it works::

    import chimerapy.engine as cpe

    # Create local objects
    manager = cpe.Manager() # using default configs
    worker = cpe.Worker(name="local") # creating local worker

    # Connect
    worker.connect(host=manager.host, port=manager.port)

For a distributed cluster, the connection setup requires more work. First, we start the :class:`Manager<chimerapy.engine.Manager>` in the main computer::

    $ python
    >>> import chimerapy as cpe
    >>> manager = cpe.Manager()
    2023-07-11 21:01:49 [DEBUG] chimerapy-engine-networking: <Server Manager>: running at 192.168.1.155:9000
    2023-07-11 21:01:49 [INFO] chimerapy-engine: Manager started at 192.168.1.155:9000


Once the :class:`Manager<chimerapy.engine.Manager>` started, the next step is to access the worker computers and use the ChimeraPy :class:`Worker<chimerapy.engine.Worker>` connect entrypoint to establish the connection. With the following command, we can connect the worker computer::

    $ # You will have to obtain your Manager's IP address (ifconfig)
    $ cp-worker --host IP_ADDRESS --port 9000 --name worker1 --id worker1

With the correct networking information (change IP_ADDRESS with the ip address of your computer hosting the :class:`Manager<chimerapy.engine.Manager>`), the :class:`Worker<chimerapy.engine.Worker>` should connect and the :class:`Manager<chimerapy.engine.Manager>` should report the :class:`Worker<chimerapy.engine.Worker>` as registered::

    2022-11-03 22:42:05 [INFO] chimerapy: <Server Manager MANAGER_MESSAGE->WORKER_MESSAGE>: Got connection from ('10.0.0.171', 44326)

This message informs us that the :class:`Worker<chimerapy.engine.Worker>` connected successfully.

Worker-Node Mapping
===================

After setting up our cluster, we need to delegate :class:`Nodes<chimerapy.engine.Node>` to the :class:`Workers<chimerapy.engine.Worker>`. This is achieved by using a :class:`Graph<chimerapy.engine.Graph>` object and a mapping between the workers and the nodes. Then, through a dictionary mapping, where the keys are the workers' names and the values are list of the nodes' names, we can specify which workers will perform which node tasks. Here is an example::

    # Then register graph to Manager
    manager.commit_graph(
        graph=graph,
        mapping={
            "local": ["source", "step"],
        }
    )

We then commit the :class:`Graph<chimerapy.engine.Graph>` to the :class:`Worker<chimerapy.engine.Worker>`. All the :class:`Nodes'<chimerapy.engine.Node>` code are located within the :class:`Manager's<chimerapy.engine.Manager>` computer; therefore, these compartmentalized code needs to be sent to the :class:`Workers<chimerapy.engine.Worker>`. The ``commit_graph`` routine can take some time based on the number of :class:`Worker<chimerapy.engine.Worker>`, :class:`Nodes<chimerapy.engine.Node>`, and their code size hence waiting until all nodes are ready.

Execution
*********

Now we are ready to execute the system, this is achieved through the :class:`Manager<chimerapy.engine.Manager>`'s control API. Below shows how to start, execute for 10 seconds, and then stop the system::

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(10)
    manager.stop()
