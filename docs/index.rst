.. chimerapy documentation master file, created by
   sphinx-quickstart on Tue Mar 29 03:41:19 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: _static/logo/chimerapy_logo_with_name_theme_blue.png
  :width: 90%
  :alt: ChimeraPy Logo

|pipy|_ |coverage|_ |test| |license| |style|

.. |pipy| image:: https://img.shields.io/pypi/v/chimerapy
   :alt: PyPI
.. _pipy: https://pypi.org/project/chimerapy/

.. |coverage| image:: https://coveralls.io/repos/github/oele-isis-vanderbilt/ChimeraPy/badge.svg?branch=main
   :alt: coverage
.. _coverage: https://coveralls.io/github/oele-isis-vanderbilt/ChimeraPy?branch=main

.. |test| image:: https://img.shields.io/github/actions/workflow/status/oele-isis-vanderbilt/ChimeraPy/.github/workflows/test.yml?branch=main
   :alt: test

.. |license| image:: https://img.shields.io/github/license/oele-isis-vanderbilt/ChimeraPy
   :alt: license

.. |style| image:: https://img.shields.io/badge/style-black-black
   :alt: style

Welcome!
========

ChimeraPy is a distributed computing framework for multimodal data dollection, processing, and feedback. It's a real-time streaming tool that leverages from a distributed cluster to empower AI-driven applications.

#. **Collect** your data in real-time from any computer and time aligned it to the rest of your data streams.
#. **Process** data as soon as it comes, providing a service to your users.
#. **Archive** your outputs and later retrieve them in a single main data archive for later careful post-hoc analysis.
#. **Monitor** the executing of your distributed cluster, view live outputs, and verify that you collected clean data.


Contents
--------

.. toctree::
   :maxdepth: 2
   :includehidden:

   getting_started
   usage/index

.. toctree::
   :maxdepth: 4
   :includehidden:

   framework/index

.. toctree::
   :maxdepth: 2
   :includehidden:

   api/index

.. toctree::
   :maxdepth: 3
   :includehidden:

   developer/index
   authors

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
