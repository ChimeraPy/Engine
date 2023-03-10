# Built-in Imports
import threading
import queue
import uuid

# Third-party
import docker
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy-networking")


class ContainerLogsCollector(threading.Thread):
    def __init__(self, name: str, stream, output_queue: queue.Queue):
        super().__init__()

        # Saving input parameters
        self.name = name
        self.stream = stream
        self.output_queue = output_queue
        self.running = threading.Event()
        self.logs = []

    def __repr__(self):
        return f"<LogThread {self.name}>"

    def run(self):
        self.running.set()
        while self.running.is_set():
            for data in self.stream:
                self.output_queue.put(data.decode())
                self.logs.append(data.decode())

    def stop(self):
        self.running.clear()

    def post_join(self):
        for data in self.logs:
            logger.debug(f"{self.name}: {data}")


class DockeredWorker:
    def __init__(self, client: docker.DockerClient, name: str):
        self.container = client.containers.run(
            image="chimerapy",
            auto_remove=False,
            stdin_open=True,
            detach=True,
            # network_mode="host", # Not realistic
        )
        self.name = name

        # Create id
        self.id: str = str(uuid.uuid4())

    def connect(self, host, port):

        # Connect worker to Manager through entrypoint
        _, stream = self.container.exec_run(
            cmd=f"cp-worker --id {self.id} --ip {host} --port {port} --name {self.name} --wport 0",
            stream=True,
        )

        # Execute worker connect
        self.output_queue = queue.Queue()
        self.log_thread = ContainerLogsCollector(self.name, stream, self.output_queue)
        self.log_thread.start()

        # # Wait until the connection is established
        while True:

            try:
                data = self.output_queue.get(timeout=10)
            except queue.Empty:
                raise RuntimeError("Connection failed")

            if "connection successful to Manager" in data:
                break

    def shutdown(self):

        # Then wait until the container is done
        self.container.kill()
        self.container.wait()
        self.log_thread.stop()
        self.log_thread.join()
        self.log_thread.post_join()
