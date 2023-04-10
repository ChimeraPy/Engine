Tutorial
########
During this tutorial, we will be using an example, specifically ``remote_camera.py`` provided within the ``examples`` folder of the ChimeraPy's GitHub repository. This example streams a video from one computer to another and displays the resulting webcam image.

.. warning::
   This example might not work if your camera permissions do not allow you to access your webcam through OpenCV's ``VideoCapture``. It is recommend that you first try the Local cluster option to test instead of Distributed, as it is easier to debug.

Data Sources and Process as Nodes
*********************************

The first step to executing a data pipeline with ChimeraPy is to design your :class:`Node<chimerapy.Node>` implementations. In this situation, we have the ``WebcamNode`` that pulls images from the webcam through OpenCV. The receiving node, ``ShowWindow`` displays the webcam video through an OpenCV display. Here are the :class:`Node<chimerapy.Node>` definitions::

    from typing import Dict, Any
    import time

    import numpy as np
    import cv2

    import chimerapy as cp

    class WebcamNode(cp.Node):
        def setup(self):
            self.vid = cv2.VideoCapture(0)

        def step(self):
            time.sleep(1 / 30)
            ret, frame = self.vid.read()
            data_chunk = cp.DataChunk()
            data_chunk.add('frame', frame, 'image')
            return data_chunk

        def teardown(self):
            self.vid.release()


    class ShowWindow(cp.Node):
        def step(self, data: Dict[str, cp.DataChunk]):
            frame = data["web"].get('frame')['value']
            cv2.imshow("frame", frame)
            cv2.waitKey(1)


An Elegent Approach: Subclassing Graph
**************************************

Now we have to define our :class:`Graph<chimerapy.Graph>`. There two main ways to achieve this: create a :class:`Graph<chimerapy.Graph>` instance and then add the connections, or create a custom :class:`Graph<chimerapy.Graph>` subclass that defines the connections. In the `Basics<basics>`, we show the first approach, therefore we will use the latter in this tutorial. Below is the ``RemoteCameraGraph``::

    class RemoteCameraGraph(cp.Graph):
        def __init__(self):
            super().__init__()
            web = WebcamNode(name="web")
            show = ShowWindow(name="show")

            self.add_nodes_from([web, show])
            self.add_edge(src=web, dst=show)

Controlling your Cluster
************************

With our DAG complete, the next step is configuring the network configuration and controlling the cluster to start and stop. Make sure, because of mulitprocessing's start methods, to wrap your main code within a ``if __name__ == "__main__"`` to avoid issues, as done below::

    if __name__ == "__main__":

        # Create default manager and desired graph
        manager = cp.Manager()
        graph = RemoteCameraGraph()
        worker = cp.Worker(name="local")

        # Then register graph to Manager
        worker.connect(host=manager.host, port=manager.port)

        # Wait until workers connect
        while True:
            q = input("All workers connected? (Y/n)")
            if q.lower() == "y":
                break

        # Distributed Cluster Option
        # mapping = {"remote": ["web"], "local": ["show"]}

        # Local Cluster Option
        mapping = {"local": ["web", "show"]}

        # Commit the graph
        manager.commit_graph(
            graph=graph,
            mapping=mapping
        )

        # Wail until user stops
        while True:
            q = input("Ready to start? (Y/n)")
            if q.lower() == "y":
                break

        manager.start()

        # Wail until user stops
        while True:
            q = input("Stop? (Y/n)")
            if q.lower() == "y":
                break

        manager.stop()
        manager.shutdown()

In this main code, we have the option to run this between two computers (the Distributed Cluster Option), in which we would have to connect another computer through the entrypoint, as the following::

    $ cp-worker --ip <manager's ip> --port <manager's port> --name remote

The easier route (to test that the system is working correctly) is to execute the DAG first in your local computer (Local Cluster Option). Now, let's walk through the logic in the main script.

#. We create the :class:`Manager<chimerapy.Manager>`, the ``RemoteCameraGraph``, and local :class:`Worker<chimerapy.Worker>`.
#. Connected :class:`Workers<chimerapy.Worker>` to :class:`Manager<chimerapy.Manager>` and provide a wait-for-user to connect remote Workers
#. Map the :class:`Graph<chimerapy.Graph>` based on either Distributed or Local cluster option
#. Committed the :class:`Graph<chimerapy.Graph>` and configured the network to deploy the DAG
#. Waits until user is ready to start executing DAG
#. With user approval, DAG is executed, streaming in real time.
#. Waits until user shutdowns sytem.

For this example, during the runtime of the DAG in ChimeraPy, your webcam (as long as permissions are setup correctly), it should display your current webcam's video in real-time.
