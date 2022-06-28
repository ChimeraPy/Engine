# Package Management
__package__ = 'chimerapy'

# Built-in imports
from typing import Dict, Optional, Union, List, Sequence
import os 
import sys
import pathlib
import threading
import signal
import time
import psutil
import collections
import queue
import multiprocessing as mp

# Third-party imports
import pandas as pd
import networkx as nx
import curses

# Local Imports
from chimerapy.utils.memory_manager import MemoryManager, MPManager
from chimerapy.core.process import Process
from chimerapy.core.session import Session
from chimerapy.utils.tools import threaded
from chimerapy.core.data_stream import DataStream
from chimerapy.core.reader import Reader
from chimerapy.core.writer import Writer

# define a new metaclass which overrides the "__call__" function
class NewInitCaller(type):
    # Reference: https://stackoverflow.com/questions/16017397/injecting-function-call-after-init-with-decorator

    def __call__(cls, *args, **kwargs):
        """Called when you call MyNewClass() """
        obj = type.__call__(cls, *args, **kwargs)
        obj._post_init_call()
        return obj
        
class Pipeline(metaclass=NewInitCaller):
        
    # Attributest that a pipeline must always have
    session = None
    graph = nx.DiGraph()
    _processes = []

    def __init__(
            self, 
            name:str, 
            inputs:List[Union[DataStream, 'Pipeline']],
            time_window:pd.Timedelta=pd.Timedelta(seconds=5),
            logdir:Optional[Union[str, pathlib.Path]]=None,
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
            memory_limit:float=0.8,
            verbose:bool=False,
        ) -> None:
        """Construct a ``Pipeline``.

        Within this construct, define the chain between your Process and 
        keep them stored as variables directly connected to your class. 
        That way we can identify the pipeline structure and create a 
        graph representation.

        Args:
            name (str): Name of the pipeline
            time_window (pd.Timedelta): The size of the time window.
            logdir (Optional[Union[str,pathlib.Path]]): The log directory \
                to use for saving and logging data to.
            start_time (Optional[pd.Timedelta]): A timestamp to define \
                the start time of the processing. Any data before the \
                ``start_time`` will be ignored.
            end_time (Optional[pd.Timedelta]): A timestamp to define \
                the end tim of the processing. Any data after the \
                ``end_time`` will be ignored.
            memory_limit (float): The percentage of the available RAM \
                that will be permitted to be used by the ``Runner``. \
                For slower computers and with small RAM, a smaller \
                ``memory_limit`` is highly recommended.
            memory_usage_factor (float): This factor is used to more \
                accurate account for the memory used by the library. Even \
                with tracking all memory loaded, the actual memory is\
                typically higher. The factor is to account for this \
                difference.
            verbose (bool): Debugging printout.

        """
        # Verify inputs
        assert isinstance(inputs, list), "``inputs`` must be a list."

        # Store input parameter
        self.name = name
        self.inputs = inputs
        self.pipelines = []
        self.dss = collections.defaultdict(DataStream) # data streams
 
        # Pipeline state information
        self.setup_executed = mp.Value('i', False)
        self.running = mp.Value('i', True)

        # Convert the logdir to pathlib
        if isinstance(logdir, str):
            self.logdir = pathlib.Path(logdir)
        elif isinstance(logdir, pathlib.Path):
            self.logdir = logdir
        else:
            self.logdir = pathlib.Path('.')

        # Store the information
        self.verbose = verbose

        # Create the directed graph and add the data streams as nodes
        for ins in self.inputs:
            # If data stream, this becomes the source
            if isinstance(ins, DataStream):
                self.graph.add_node(ins.name, item=ins)
                self.dss[ins.name] = ins

            # If it is another pipeline, we have to add it to the current
            # pipeline's graph and know all of its the previous content
            elif type(ins) == 'Pipeline':

                # Add the pipeline to this pipeline, with an suffix
                self.safe_merge(ins)

                # Store the pipeline to avoid in the future adding it again
                self.pipelines.append(ins)
        
        # Create memory manager that is shared between processes
        self.base_manager = MPManager()
        self.base_manager.start()
        self.memory_manager = MemoryManager(
            memory_limit=memory_limit
        )
        
        # Setup the reader
        self.init_reader(
            self.dss,
            self.memory_manager,
            time_window,
            start_time,
            end_time
        )

        # Setup the writer
        self.init_writer(self.memory_manager)

        # Create session for runner
        self.session = Session(
            name='root',
            queue=self.writer.out_queue,
            memory_manager=self.memory_manager
        )
        
        # Define the protocol for processing messages from the reader and writer
        self.respond_message_protocol = {
            'reader':{
                'UPDATE': {
                    'TIMETRACK': self.respond_reader_message_timetrack,
                },
                'META': {
                    'END': self.respond_reader_message_end
                }
            },
            'writer':{
                'META': {
                    'END': self.respond_writer_message_end
                }
            }
        }
        
        # Create the communication threads
        self.message_reader_thread = self.check_reader_messages()
        self.message_writer_thread = self.check_writer_messages()
        
    def _post_init_call(self) -> None:

        # Check for all the set attributes in the Pipeline
        for attr_name, attr in self.__dict__.items():

            # If it is a process, add it to the graph and its edge
            if type(attr) == Process:

                # Add the proces to the list to track them all
                self._processes.append(attr)

                # Add the process as a node
                self.graph.add_node(attr.name, item=attr)

                # Add the edges from the process
                for ins in attr.inputs:
                    self.graph.add_edge(ins.name, attr.name)

            # Else if it is a pipeline, add its subgraph
            if type(attr) == 'Pipeline':

                # Check if this is a previously added pipeline
                if attr not in self.pipelines:
                    self.safe_merge(attr)

        # Then construct a layered graph
        self.layers = []
        self.nodes_registry = {}

        # The zeroth layer is all the data streams
        all_dss_nodes = [x for x in self.graph.nodes(data=True) if isinstance(x[1]['item'], DataStream)]
        self.layers.append(all_dss_nodes)

        # Setup the layer id used to iterate through
        i = 1

        # Then the following layers are constructed 
        while True:

            # Get all the next layer descendants
            next_layer_successors = []
            for node in self.layers[i-1]:

                # Obtain the successors with their 'item' (ds or process)
                successors= list(self.graph.successors(node[0]))
                node_view_successors = [(n, {'item': self.graph.nodes[n]['item']}) for n in successors]

                # Remove the node if it appeared in a previous layer
                for next_node in node_view_successors:
                    if next_node[0] in list(self.nodes_registry.keys()):
                        # Find the previous layer
                        previous_layer_id = self.nodes_registry[next_node[0]]
                        # Remove from that layer
                        self.layers[previous_layer_id].remove(next_node)
               
                # Then finally add all of the new node successors
                next_layer_successors += node_view_successors

            # If the next layer is empty, stop
            if len(next_layer_successors) == 0:
                break
            
            # Add it to the node registry
            for node in next_layer_successors:
                self.nodes_registry[node[0]] = i

            # If not empty, then store them and use them as the current layer
            self.layers.append(next_layer_successors)

            # Update the next layer id
            i += 1

        return None

    def add_graph_node(self, node:Process):
        self.graph.add_node(node)
    
    def add_graph_edge(self, from_node:Process, to_node:Process):
        self.graph.add_edge(from_node, to_node)
    
    def safe_merge(self, other:'Pipeline'):

        # Get the previous graph's nodes
        nodes = list(other.graph.nodes(data=False))

        # Get all the items (Process and DataStreams) from the nodes
        other_dss = [x for x in nx.get_node_attributes(other.graph, 'item') if isinstance(x, DataStream)]
        self.dss[other.name] = other_dss

        # Create a rename mapping
        new_node_names = [f"{other.name}_{n}" for n in nodes]
        rename_mapping = {k:v for k,v in zip(new_node_names, nodes)}
        
        # Add the proces to the list to track them all
        other_process = [x for x in nx.get_node_attributes(other.graph, 'item') if isinstance(x, Process)]
        self._processes += other_process

        # Store the previous graph into the new graph
        self.graph = nx.compose(nx.relabel_nodes(other.graph, rename_mapping), self.graph)

    def __str__(self) -> str:
        """Get string representation of ``Pipeline``.

        Returns:
            str: string representation.

        """
        return self.__repr__()

    def __eq__(self, other:'Pipeline') -> bool:
        return nx.is_isomorphic(self.graph, other.graph)

    def __cmp__(self, other:'Pipeline'):
        if self.name < other.name:
            return -1
        elif self.name > other.name:
            return 1
        else:
            return 0

    def respond_reader_message_timetrack(self, timetrack:pd.DataFrame, windows:list):
        """Respond to the retrieved message with initialization info.

        Args:
            timetrack (pd.DataFrame): The global timetrack generated by
            the ``Collector``.

            windows (list): List of the windows with their start and end
            times.

        """
        self.timetrack = timetrack
        self.num_of_windows = len(windows)

    def respond_reader_message_end(self):
        """Respond to the ``Reader`` inform that it has ended."""
        self.reader_finished = True

    def respond_writer_message_end(self):
        """Respond to the ``Writer`` inform that it has ended."""
        self.writer_finished = True
    
    @threaded
    def check_reader_messages(self):
        """Message thread checks for new messages and limit memory usage.
        
        The ``Reader`` message queues are checked frequently and if 
        there is a new message, then the corresponding function is 
        executed. These functions are defined by the protocols layed out 
        in the ``__init__``. 

        Given the memory usage information from the ``Reader`` and 
        ``Writer``, this thread enforces the memory limit by pausing and
        resuming the operation of each process. ``Reader`` consumes
        memory while ``Writer`` frees the RAM memory back to the system 
        to use.

        """
        # Set the flag to check if new message
        reader_message = None
        reader_message_new = False
        
        # Constantly check for messages
        while self.running.value:

            # Checking the loading message queue
            try:
                reader_message = self.reader.get_message(timeout=0.1)
                reader_message_new = True
            except queue.Empty:
                reader_message_new = False

            # Processing new loading messages
            if reader_message_new:

                # Printing if verbose
                if self.verbose:
                    print("NEW LOADING MESSAGE: ", reader_message)

                # Obtain the function and execute it, by passing the 
                # message
                func = self.respond_message_protocol['reader'][reader_message['header']][reader_message['body']['type']]

                # Execute the function and pass the message
                func(**reader_message['body']['content'])

    @threaded
    def check_writer_messages(self):
        """Message thread checks for new messages and limit memory usage.
        
        The ``Writer`` message queues are checked frequently and if 
        there is a new message, then the corresponding function is 
        executed. These functions are defined by the protocols layed out 
        in the ``__init__``. 

        Given the memory usage information from the ``Reader`` and 
        ``Writer``, this thread enforces the memory limit by pausing and
        resuming the operation of each process. ``Reader`` consumes
        memory while ``Writer`` frees the RAM memory back to the system 
        to use.

        """
        # Set the flag to check if new message
        writer_message = None
        writer_message_new = False
        
        # Constantly check for messages
        while self.running.value:
            # Checking the logging message queue
            try:
                writer_message = self.writer.get_message(timeout=0.1)
                writer_message_new = True
            except queue.Empty:
                writer_message_new = False

            # Processing new sorting messages
            if writer_message_new:

                # Obtain the function and execute it, by passing the 
                # message
                func = self.respond_message_protocol['writer'][writer_message['header']][writer_message['body']['type']]

                # Execute the function and pass the message
                func(**writer_message['body']['content'])
    
    def init_reader(
            self, 
            data_streams:Dict[str, DataStream],
            memory_manager:MemoryManager,
            time_window:pd.Timedelta,
            start_time:Optional[pd.Timedelta],
            end_time:Optional[pd.Timedelta]
        ) -> None:
        """Routine for initializing the ``Reader``.

        Args:
            time_window (pd.Timedelta): Size of the time window.
            max_loading_queue_size (int): Max size of the loading queue.

            start_time (Optional[pd.Timedelta]): The cutoff of the start 
            time of the global timetrack.

            end_time (Optional[pd.Timedelta]): The cutoff of the end 
            time of the global timetrack.

        """
        # Create variables to track the reader's data
        self.num_of_windows = 0
        self.latest_window_loaded = 0
        self.reader_finished = False
        self.reader_paused = False

        # Create the Reader with the specific parameters
        self.reader = Reader(
            data_streams=data_streams,
            memory_manager=memory_manager,
            time_window=time_window,
            start_time=start_time,
            end_time=end_time,
        )

    def init_writer(
            self,
            memory_manager:MemoryManager
        ) -> None:
        """Routine for initializing the ``Writer``."""

        # Creating variables to track the writer's data
        self.num_of_logged_data = 0
        self.writer_finished = False

        # Create the writer
        self.writer = Writer(
            memory_manager=memory_manager,
            logdir=self.logdir,
            experiment_name=self.name,
        )
        

    def setup(self) -> None:
        """Setup the ``Runner``'s run.

        The setup includes the messaging threads, the ``Reader``, the
        ``Writer``, and the additional variables to coordinate these
        multiprocessing and multithreaded components.

        """

        # Set the readers and writers up
        self.reader.setup()
        self.writer.setup()

        # Start the reader and writer
        self.reader.start()
        self.writer.start()

        # Start the message threads
        self.message_reader_thread.start()
        self.message_writer_thread.start()

        self.setup_executed.value = True

    def shutdown(self) -> None:
        """Shutting down the ``Pipeline``.

        This shutdown also includes the messasing threads, ``Writer`` 
        and ``Reader`` processes, and clearing all the messages from the
        queues. Note: processes cannot be complete ``join`` until all
        connected queues are cleared.

        """
        # Notify other methods that teardown is ongoing
        self.running.value = False

    def join(self) -> None:
        
        # # Stop the reader and writer
        self.reader.shutdown()
        if self.reader.is_alive():
            self.reader.join()

        self.writer.shutdown()
        if self.writer.is_alive():
            self.writer.join()

        # Shutdown the other processes
        for process in self._processes:
            process.shutdown()
            if process.is_alive():
                process.join()

        # Wait for the message threads to stop
        if self.message_reader_thread.is_alive():
            self.message_reader_thread.join()
        if self.message_writer_thread.is_alive():
            self.message_writer_thread.join()

    def forward(self, all_data_samples: Dict[str, pd.DataFrame]):

        # Pass the outputs through the layers
        for layer in self.layers[1:]:

            # Pass the inputs to the processes in the layer
            for process_node in layer:
                # Get the actual process
                process = process_node[1]['item']

                # Obtain all the input needed
                process_inputs = {}
                for ins in process.inputs:
                    process_inputs[ins.name] = all_data_samples[ins.name]

                # Step the process, catch errors and properly shutdown if so
                try:
                    process.step(process_inputs)
                except Exception as e:
                    self.shutdown()
                    self.join()
                    raise

            # Get the output of the processes in the layer
            for process_node in layer:
                process = process_node[1]['item']

                # Get the output
                try:
                    process_output = process.get(timeout=10)
                except queue.Empty:
                    self.shutdown()
                    self.join()
                    raise RuntimeError(f'{process} time execution timeout')

                # Store the output for other processes to use
                all_data_samples[process.name] = process_output

    def step(self) -> bool:

        # If the setup has not been executed it, run it now
        if not self.setup_executed.value:
            self.setup()

        # Retrieveing sample from the loading queue
        try:
            data_chunk = self.reader.get(timeout=1)
        except queue.Empty:
            return True

        # Check for end condition
        if (data_chunk == 'END' or data_chunk is None):
            return False

        # Decompose the data chunk
        all_data_samples = data_chunk['data'] 
        # Track the memory
        self.memory_manager.remove(data_chunk)

        # Then propagate the sample throughout the pipe
        self.forward(all_data_samples)

        return True

    def pipeline_loop(self):
        """Main routine for loading and processing data chunks.

        This routine acts as the base routine for ``get``ting data 
        chunks from the ``loading_queue`` and passing them through the
        ``Pipeline``.

        """
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        
        # Continue iterating
        while self.running.value:

            # Take a step
            to_continue = self.step()

            # Stop the loop if end
            if not to_continue:
                break

            # Increase the counter
            self.num_processed_data_chunks += 1
        
    def tui_main(self, stdscr):
        """Routine for TUI.

        The routine for the TUI updates the information from each
        process, such as the ``Reader`` and ``Writer``. Additionally,
        the memory usage is reported as well.

        """
        # Enable scrolling and setting timeouts
        stdscr.scrollok(1)
        stdscr.timeout(1)

        # Outside of while loop, generate useful setup
        process_stats = {
            'Pipeline': psutil.Process(os.getpid()),
            'Reader': psutil.Process(self.reader.pid),
            'Writer': psutil.Process(self.writer.pid)
        }

        # Continue the TUI until the other threads are complete.
        while self.running.value:

            # Generate the report for the threads of the Reader
            cpu_usage_string = ''
            processes_cpu_info = collections.defaultdict(dict)

            # For each process, generate a string about their process cpu
            # consumption and thread cpu consumption
            for i, (process, stats) in enumerate(process_stats.items()):

                # Check if the process is still alive
                if not stats.is_running():
                    # Add terminal info
                    if i == 0:
                        cpu_usage_string += f"{process}: Terminal\n"
                    else:
                        cpu_usage_string += f"\t\t    {process}: Terminal\n"
                    # Skip the rest of for loop
                    continue

                # Get process data
                try:
                    processes_cpu_info[process]['process'] = process_stats[process].cpu_percent() / psutil.cpu_count()
                except psutil.NoSuchProcess:
                    continue

                # Append process string to cpu_usage_string
                if i == 0:
                    cpu_usage_string += f"{process}: {processes_cpu_info[process]['process']:.2f}\n"
                else:
                    cpu_usage_string += f"\t\t    {process}: {processes_cpu_info[process]['process']:.2f}\n"

            # Create information string
            data_pipeline_info_str = f"""\
            Data Pipeline Information
                Reading:
                    Read Data: {self.latest_window_loaded}/{self.num_of_windows}
                    Queue Waitlist: {self.reader.data_from_queue.qsize()}
                Processing: 
                    Processed Data: {self.num_processed_data_chunks}/{self.num_of_windows}
                Writing:
                    Written Data: {self.num_of_logged_data}
                    Queue Waitlist: {self.writer.data_to_queue.qsize()}
            System Information:
                Memory Usage: {(self.memory_manager.total_memory_used/self.memory_manager.total_available_memory):.2f}
                CPU Usage: 
                    {cpu_usage_string}
            """

            # Info about data loading
            stdscr.addstr(0,0, data_pipeline_info_str)

            # Refresh the screen
            stdscr.refresh()

            # Sleep
            time.sleep(0.1)

            # Break Condition
            if self.reader_finished and \
                self.num_processed_data_chunks == self.num_of_windows and \
                self.writer_finished:
                break
    
    def run(self, verbose:bool=False) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include a TUI for tracking the \
                progress of loading, processing, and logging of data \
                throughout the data pipeline process.

        """
        # Performing setup for the subprocesses and threads
        self.setup()
 
        # If verbose, create a simple TUI showing the current state of 
        # the whole process.
        if verbose:
            tui_thread = threading.Thread(target=curses.wrapper, args=(self.tui_main,))
            tui_thread.start()

        # Execute the processing in the main thread
        self.pipeline_loop()
        
        # Then fully shutting down the subprocesses and threads
        self.shutdown()
