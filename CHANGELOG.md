# ChimeraPy CHANGELOG

## 0.0.8 (2023-01-23)

#### New Features

* (logging): Made logging available outside of ChimeraPy, made testing of Nodes easier, simplified the Manager's API, make the worker transfer local files through moving files, fixed WIndows timeout bugs.
* (logging): Added feature where DEBUG messages from ChimeraPy can be disabled from outside the package.
* (meta): Added Manager saving meta data to the logdir.
* (transfer): Made the Worker transfer its archives to the Manager.
* (tabular): Added tabular record to Node API along with pandas as a dependency.
* (audio): Adding the functionality to record audio through the Node API and multithreading.
* (data): Started the development of the Node API to save video, audio, tabular, and images during the cluster execution. Using PyAudio (had to add to setup.cfg and Dockerfile) for audio capture. Added SaveHandler as the thread to save incoming data. Made tests for Record and its children classes.
* (node): Added to the Graph API the ability to specify which Node should be followed during the execution of a step function. Now Nodes can specify if they want to execute according to a high-frequency or low-frequency input data source.
* (windows): Made all test pass on Windows.
#### Fixes

* (Windows): logging and networking correction.
* (Windows): Fixing that Windows has restrictions on valid characters for the directory names.
* (transfer): Added wait for nodes to finish saving data before starting the transfer process, changed the API to incorporate waits within the Manager's user API, and updated documentation to reflect this. Updated the version to 0.0.6.
* (docker): Change in log printouts caused mock worker to fail. Fixed the issue.
* (imports): Made dill recursively include members of Node, thereby removing errors caused by missing global variables.
* (graph): Accidently removed add_edges_from method, causing tests to fail.
* (dependencies): adding ``multiprocess`` dependency to ``setup.cfg``.
* (windows): Change ``Node`` parent from ``multiprocessing.Process`` to ``multiprocess.Process``, which is a fork of the build-in library that uses ``dill`` instead of ``pickle``. Passed the test of having the ``Manager`` and ``Worker`` being in separate terminals. Now we need to test between computers.
#### Performance improvements

* (transfer): removed the use of jsonpickle to increase performance and allow the streaming of full resolution video.
#### Refactorings

* (networking): Refactored the server and client to avoid code repetition. Handled exceptions better when it comes to networking. Added initial package distribution system within the Manager and Worker.
* (test): Started relocating the data nodes to separate file and began the testing of data transfer.
#### Docs

* (dependencies): Started creating documentation on how to deal with dependencies used by a custom data pipeline.
* (windows): Removed the warning in README.md that stated ChimeraPy does not work with Windows. Now Windows, Linux, and MacOS are supported.
* (changelog): added automatic CHANGELOG GitHub Actions Workflow, fixed PYPI logo rendering, and added some links in the developer docs
#### Others

* (examples): Updated examples to use the new Manager API.
* (PIPY): Made README.md work with PIPY.
* (coverage): Made coverage account for multiprocessing and multithreading.
* (version): Updated version and fixed PYPI image loading issue.
* (coverage): Made the coverage command account for multiprocessing.
* (version): updating version to 0.0.4
* (passing): Finally made all test pass with the new networking component.
* (p2p): Got the p2p setup test passing.
* (transfer): Added more test (docker) for the file transfer between Manager and Worker.
