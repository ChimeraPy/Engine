# Built-in Imports
import time
import queue
import multiprocessing as mp
from multiprocessing.managers import BaseManager, NamespaceProxy

# Third-party Imports
import pytest

# ChimeraPy Imports
import chimerapy as cp
# from chimerapy.core.process import ProcessManager

@pytest.mark.xfail
def test_process_attribute_access():

    class SimpleProcess(mp.Process):

        def __init__(self, a):
            super().__init__()
            self.a = a

        def run(self):

            while True:
                if self.a == 2:
                    break
                else:
                    time.sleep(0.1)

        def set_a(self, a):
            self.a = a

        def get_a(self):
            return self.a

    # Create the instance
    p1 = SimpleProcess(a=1)

    # Start the process
    p1.start()

    # Wait for a little
    time.sleep(1)

    # Then change a and see if it stops the process
    p1.set_a(2)

    # Wait for a little
    time.sleep(1)

    assert p1.get_a() == 2
    p1.terminate()

def test_shared_value_between_parent_and_child():
    # References:
    # https://superfastpython.com/share-process-attributes/

    class SimpleProcess(mp.Process):

        def __init__(self, a):
            super().__init__()
            self.a = mp.Value('i', a)

        def run(self):

            while True:
                if self.a.value == 2:
                    break
                else:
                    time.sleep(0.1)

        def set_a(self, a):
            self.a.value = a

        def get_a(self):
            return self.a.value

    # Create the instance
    p1 = SimpleProcess(a=1)

    # Start the process
    p1.start()

    # Wait for a little
    time.sleep(1)

    # Then change a and see if it stops the process
    p1.set_a(2)

    # Wait for a little
    # time.sleep(1)
    p1.join()

    assert p1.get_a() == 2

def test_process_shutdown():

    # Create a process
    p1 = cp.Process(name='test', inputs=None)

    # Print the original memory value
    assert p1.get_running() == False

    # Start the process
    p1.start()

    # Change it, testing that the shared memory is working
    p1.shutdown()

    # Wait until the process stops
    p1.join()
    
    # assert p1.get_running() == True
    assert p1.get_running() == True

def test_forwarding():

    class ForwardingProcess(cp.Process):

        def step(self, data_chunk):
            return data_chunk
    
    # Create the process
    p1 = ForwardingProcess(name='test', inputs=None)
    p1.start()

    # Test message
    data_chunk = [1,2,3,4]

    # Send a message to the Process
    p1.put(data_chunk)

    # Receive message
    output = p1.get(timeout=1)

    # Shutting down the process
    p1.shutdown()
    p1.join()

    assert output == data_chunk

def test_simple_operation():

    class SummingValues(cp.Process):

        def step(self, data_chunk):
            return sum(data_chunk)
    
    # Create the process
    p1 = SummingValues(name='test', inputs=None)
    p1.start()

    # Test message
    data_chunk = [1,2,3]

    # Send a message to the Process
    p1.put(data_chunk)

    # Receive message
    output = p1.get(timeout=1)

    # Shutting down the process
    p1.shutdown()
    p1.join()

    assert sum(data_chunk) == output

def test_simple_message_comm():

    class CommProcess(cp.Process):
        
        def __init__(self):
            super().__init__(name='test', inputs=None, verbose=True)
            self.subclass_message_to_functions.update({'HELLO': self.hello})
        
        def hello(self, *args, **kwargs):
            self.message_from_queue.put('hello')

    # Create the process
    p1 = CommProcess()
    p1.start()

    # Send a message with the HELLO protocol
    message = {
        'header': 'TEST',
        'body': {
            'type': 'HELLO',
            'content': {}
        }
    }
    p1.put_message(message)

    # Then check for a message
    output_message = p1.get_message(timeout=5)
    
    # Shutting down the process
    p1.shutdown()
    p1.join()

    # Check
    assert output_message == 'hello'

def test_pausing_and_resume():

    class SummingValues(cp.Process):

        def step(self, data_chunk):
            return sum(data_chunk)
    
    # Create the process
    p1 = SummingValues(name='test', inputs=None)
    p1.start()

    # Test message
    data_chunk = [1,2,3]

    # Send a message to the Process
    p1.pause()
    p1.put(data_chunk)

    # Receive message
    try:
        output = p1.get(timeout=1)
        print(output)
        assert False, "This should timeout as no output is generated yet"
    except queue.Empty:
        time.sleep(0.5)

    # Then resume
    p1.resume()
    output = p1.get(timeout=1)

    # Shutting down the process
    p1.shutdown()
    p1.join()

    assert sum(data_chunk) == output

def test_proactive_process():

    class ProactiveProcess(cp.Process):

        def __init__(self):
            super().__init__(name='test', inputs=None, run_type='proactive')
            self.a = mp.Value('i', 1)
            self.subclass_message_to_functions.update({'UPDATE': self.update_a})

        def step(self):
            time.sleep(0.1)
            print(self.a.value)
            return self.a.value

        def update_a(self, a):
            print(f"Updating a: {a}")
            self.a.value = a

    # Create the process
    p1 = ProactiveProcess()
    p1.start()

    # Let the current value of a=1 create some output
    time.sleep(1)

    # Send a message with the HELLO protocol
    message = {
        'header': 'TEST',
        'body': {
            'type': 'UPDATE',
            'content': {'a': 2}
        }
    }
    p1.put_message(message)

    # Wait for the process to generate some output
    time.sleep(1)

    # Stop the process but keep the queues
    p1.pause()

    # Get all the output
    outputs = []
    while True:
        try:
            output = p1.get(timeout=1)
            outputs.append(output)
        except queue.Empty:
            break
 
    # Shutting down the process
    p1.shutdown()
    p1.join()
  
    # Check
    assert outputs[-1] == 2
