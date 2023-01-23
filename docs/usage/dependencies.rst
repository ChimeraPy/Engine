Dependencies
############

Handling Python dependencies across multiple computers is a challenge. As of ``v0.0.8``, dependencies (that are available via package managers) needs to be installed in the other computers. Other packages that are being developed alongside the development of ChimeraPy, typically packages still not published in package managers like ``pip`` or ``conda`` can be handled by ChimeraPy. This can be achieved by specifying package information during commit the graph to the cluster via the ``cp.Manager``. Here is an example::

    manager.commit_graph(
        graph=test_graph,
        mapping={"worker1": ["node1", "node2"]}
        send_packages=[
            {"name": "test_package", "path": pathlib.Path.cwd() / "test_package"}
        ],
    )

By using the ``send_packages`` parameter, we can specify the name and location of the local package.

.. warning::
   Using this method of distributing packages is limited, as we cannot transfer packages that rely on ``.so`` files. This is because of the limitation of `zip imports <https://realpython.com/python-zip-import/#understand-the-limitations-of-zip-imports>`_.
