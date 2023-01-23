=======================
Developer
=======================

Hello! Glad that you are interested in contributing to this open-source project. Reading the :ref:`Framework documentation<framework>`  provides an overview in how ChimeraPy works, which should be a good starting point. Additionally, the API documentation helps describe the source code.

Environment Setup
=======================

To create a development environment, use instead the following command::

    pip install '.[test]'

With this command, the necessary dependencies for testing will be automatically installed. There are also the following options: ``[docs, benchmark, examples]`` installation add-ons.

To execute the test, use the following command (located within the root GitHub repository folder)::

    pytest

In this project, we use the ``pre-commit`` library to verify every commit follows certain standards. This dependency is automatically included in the test dependency installation. `The pre-commit homepage <https://pre-commit.com>`_ provides information in how its used and advantages.

Documentation
=============

.. _developerdocs:

Below is the developer documentation, which includes all the classes,
methods, and modules used by ChimeraPy.

.. toctree::
   :maxdepth: 2
   :includehidden:

   changelog.rst
   communication.rst
   actors.rst
   utils.rst
   docker.rst
   docs.rst
