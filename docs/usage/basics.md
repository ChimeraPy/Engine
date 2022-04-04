# Basics

ChimeraPy is a library that provides the utilities to easily process
multiple streams of data through pipelines, while also logging data in
an orderly fashion. 

## Data Streams and Synchronization

ChimeraPy currently supports video and tabular data streams. These 
data modalities are implemented through ``chimerapy.VideoDataStream`` 
and ``chimerapy.TabularDataStream`` respectively. These data streams
require all the necessary input parameters to perform data fusion
and synchronization. 

Below is an example of instantication of the two classes.

```python
import pandas as pd

csv_data = pd.read_csv('example.csv')
video_path = './example/video.avi'

# Create each type of data stream
self.tabular_ds = cp.TabularDataStream(
    name="test_tabular",
    data=csv_data,
    time_column="_time_"
)
self.video_ds = cp.VideoDataStream(
    name="test_video",
    start_time=pd.Timedelta(0),
    video_path=video_path
    fps=30
)

```

Once we create the data streams, they are passed to ``SingleRunner`` 
or ``GroupRunner`` to them to be used as input to the data pipeline.

## Temporal Loading

After synchronizing all the data streams, ChimeraPy loads multimodal
data through a time window fashion. This is more elaborated in the 
[Overview](../framework/index.rst). A certain time window is used to
obtain the data from all data streams ranging from the start to the
end of the window. By loading and processing a time window instead of 
single data points, the processing effiency is greatly improved.

## Pipelines and Processes

The pipeline in ChimeraPy is 100% in control by the you. Simply 
subclass ``chimerapy.Pipeline`` and implemented three methods: ``start``,
``step``, and ``close``. **Note**: Only is ``step`` implementation is 
required. Here is an pipeline example:

```python
# Creating test pipe class and instance
class TestExamplePipeline(cp.Pipeline):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        self.session.add_tabular('test_tabular', data_samples['test_tabular'])
        self.session.add_video('test_video', data_samples['test_video'])
        self.session.add_images('test_images', data_samples['test_video'])
```

It is important to know that all pipelines have a ``session`` attribute
that provides a method for logging data through the data processing.

For processes, ChimeraPy provides a simple modular structure named
``Process``, which also relies on the use of of the ``step`` function.

## Logging and ChimeraDash

As shown in the previous section, the logging is perform by the attribute
``Pipeline.session``. Currently, the ``session`` can only perform the
following logging: ``add_tabular``, ``add_video``, and ``add_images``.
Through the pipeline process the data, the logged data will be saved
in a directory that can be later used with ChimeraDash.

ChimeraDash is a multimodal dashboard (a really fancy video player) that
plays multiple data streams at once. It is helpful for evaluating your
pipeline's output. Below is a screenshot:

![ChimeraDash](../_static/ChimeraDash.png)

## Runners (Single or Group)

All of the previous ChimeraPy library components require the 
``SingleRunner`` and/or ``GroupRunner`` to execute your data pipeline.
The main difference of runners is that ``SingleRunner`` is intended used
for scenarios with one object or person of interest. ``GroupRunner`` is
intended for grouping multiple ``SingleRunner`` when dealing with 
scenarios with multiple objects of interest.

The runner classes put everything together and then start the entire
process. Below are examples for constructing a ``SingleRunner``:

```python

# Load construct the first runner
self.runner = cp.SingleRunner(
    logdir=OUTPUT_DIR,
    name='P01',
    data_streams=[self.tabular_ds, self.video_ds],
    pipe=self.individual_pipeline,
    time_window=pd.Timedelta(seconds=1),
    end_time=end_time,
    run_solo=True,
    memory_limit=0.8
)
```

And here is an example of constructing a ``GroupRunner``:

```python
# Then for each participant, we need to setup their own session,
# pipeline, and runner
self.runners = []
for x in range(2):
    
    # Use a test pipeline
    individual_pipeline = TestExamplePipeline()

    runner = cp.SingleRunner(
        name=f"P0{x}",
        data_streams=dss.copy(),
        pipe=individual_pipeline,
    )

    # Store the individual's runner to a list 
    self.runners.append(runner)

# Load construct the first runner
self.runner = cp.GroupRunner(
    logdir=OUTPUT_DIR,
    name="ChimeraPy",
    pipe=cp.Pipeline(),
    runners=self.runners, 
    # end_time=pd.Timedelta(seconds=5),
    time_window=pd.Timedelta(seconds=0.5),
    memory_limit=0.5
)
```
