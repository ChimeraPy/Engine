Debugging Nodes
#################

Before executing the cluster and rolling a complex system to handle streams of data, it's best to perform unittest for each Node. By setting the parameter ``debug`` to ``True`` in the constructor of the :class:`Node<chimerapy.Node>`, we can begin testing only 1 node. Here is an example::

    # Imports
    import chimerapy as cp
    import time

    class TestingVideoNode(cp.Node):

        def __init__(self, name:str, id: int = 0, debug: bool = False):
            # Do not forget to include parent's arguments!
            super().__init__(name, debug)

            # Saving additional input parameters
            self.id = id

        def setup(self):
            # Create the generator to be used later
            self.vid = cap.VideoCapture(self.id)

        def step(self):
            # Set the execution to 1 every second
            time.sleep(1)
            ret, frame = cap.read()
            # To transmit video, you need to use DataChunk directly
            data_chunk = cp.DataChunk()
            data_chunk.add('frame', frame, 'image')
            return data_chunk

        def teardown(self):
            # Closing the video capture
            self.vid.release()

.. warning::
    When testing the Nodes, make sure to executing ``shutdown`` and/or ``join`` when using ``step`` or ``start``.

There are two main methods for testing your code: step or streaming. It is recommend to start with step execution to test. Here is how we can test it in a step manner::

    # Creating the Node
    test_node = TestingVideoNode('test', 0, True)

    # Here we can test the step
    for i in range(10):
        test_node.step()

    # Make sure to shutdown the Node
    test_node.shutdown()

For the streaming testing, instead of executing the Node within the main current process, we are executing the code in a separate subprocess. This makes debugging, logging, and error tracking much harder but it is closer to the execution of the cluster. It's still a good step to test before its integration into the cluster. Here is the code to test it::

    # Creating the Node
    test_node = TestingVideoNode('test', 0, True)

    # Here we can test it during streaming (non-blocking)
    test_node.start()

    # Wait for 10 seconds
    time.sleep(10)

    # Shutdown
    test_node.shutdown()
    test_node.join() # Make sure to join the Node (as it is a subprocess)
