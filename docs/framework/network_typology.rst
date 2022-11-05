Network Typology
################

.. _network:

.. image:: ../_static/architecture/Architecture.png
  :width: 90%
  :alt: Network Typology Types

ChimeraPy uses the server-client architecture as a means to provide researchers, engineers, and developers with a cluster controller, where they can setup, start, stop, pause, and resume cluster operations. The main server of ChimeraPy broadcasts this control operations to the entire cluster.

To then model data pipelines, a peer-to-peer (P2P) typology is put in place. The P2P can be extended to present graphs, as they both share the freedom of establishing edges between any two nodes.

.. image:: ../_static/architecture/SystemDesign.png
  :width: 90%
  :alt: System Design

The combination of these two network typologies results in the diagram above. This is the resulting network design of ChimeraPy. In this case, the cluster's overall Server is the :class:`Manager<chimerapy.Manager>` and the Client is the :class:`Worker<chimerapy.Worker>`. The P2P network is embodied by computer processes instead of unique computers. These processes are linked through the scaffolding of the :class:`Worker<chimerapy.Worker>` and :class:`Manager<chimerapy.Manager>`.
