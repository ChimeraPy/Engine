Docker
======

To test distributed systems within a single host machine, Docker was used to spin up virtual machines that would simulate local computers.

.. warning::
    Must of the Docker development was done in Linux (Ubuntu 22.04). Mileage will vary when testing, developing, and using Docker components in other operating systems.

    Make sure you can install Docker in your machine before continuing.

You will also need sudo access to run Docker. Use the `Offical Docker instructions <https://docs.docker.com/engine/install/linux-postinstall/#:~:text=Manage%20Docker%20as%20a%20non-root%20user&text=If%20you%20don't%20want,members%20of%20the%20docker%20group.>`_ to give permissions to your user to execute Docker without the need of ``sudo``.

Docker Image
------------

To start, create the Docker image ``chimerapy`` with the following command::

    docker build -t chimerapy .

This uses the ``Dockerfile`` in the ChimeraPy's GitHub repository to create an image with all the necessary dependencies. We will use this image to later create container for our manual and automated testing.

Docker Container and Shell Access
---------------------------------

For automated testing, creating the Docker image is sufficient. For manual testing, you can continue with these set of instructions. To avoid having to manage containers, we are going to use the following command to create a Docker container that's ready for testing::

    sudo docker run --rm -it --entrypoint /bin/bash chimerapy

With this command, you should be able to have access to the terminal of the Docker container. It should look something like this::

    root@ce203d4b7798:/#

This is according to this answer in `SO answer <https://stackoverflow.com/a/53220818/13231446>`_.

Running ChimeraPy Worker
------------------------

With our Docker container ready, we can start testing ChimeraPy in a more realistic setting. The ``examples`` folder is a good start for trying out ChimeraPy's functionality. More than likely, the Docker container will need to execute the Worker entrypoint, like this::

    root@ce203d4b7798:/# cp-worker --ip 10.0.171 --port 9000 --name remote
