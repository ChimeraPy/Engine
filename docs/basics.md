# Basics 
The architecture can be broken down to two main components: before and after the ``Collector``. Before the ``Collector`` the data is handled indepedent of each other - in a unimodal fashion. After the integration of the ``Collector``, multimodal processing can occur. This is done by the use of a global timetrack that helps determine the time order of data from various data sources. The ``Analyzer`` then takes the chronologically ordered data from the collector and propagates data samples through their respective data pipelines. At the end, the analyzer uses the ``Session`` instance to store the generated multimodal data streams.

### [Data Stream](api-docs/data_stream.md)

The ``DataStream`` class is a fundamental building for this library. It contains the necessary methods and attributes to help organize the data modalities.

### [Process](api-docs/process.md)

The ``Process`` class is a compartmentalized computation operation that is applied to input data streams. A process can be used for both the indepedent pre-fusion or mixed post-fusion processing stages. 

### [Collector](api-docs/collector.md)

The ``Collector`` class has one goal: align the timing between the incoming data streams and provide data samples to the ``Analyzer`` in a chronological form.

### [Analyzer](api-docs/analyzer.md)

The ``Analyzer`` class is the main component of the pipeline. The ``Analyzer`` constructs the post-fusion data pipelines for each type of data stream in the ``Collector``. As a new data sample is passed from the ``Collector`` to the ``Analyzer``, the ``Analyzer`` feeds the sample to its respective pipeline and stores intermediate and final data points in its ``Session`` attribute.

## Modality-Specific Toolkits

The goal of PyMMDT is to provide general toolkits for each type of popular data modality. Currently, only video and tabular logs toolkits are available in ``mm.video`` and ``mm.logs`` subpackages.

#### Video

The video subpackage is home for useful implementations of ``DataStream`` and ``Process`` classes that are specially catered to video data. 

#### Tabular Logs

The tabular logs subpackage is focused on CSV, Excel and other tabular forms of log data. These include implementations of ``DataStream`` with additional useful methods.
