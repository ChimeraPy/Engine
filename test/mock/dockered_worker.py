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
        self.daemon = True

    def __repr__(self):
        return f"<LogThread {self.name}>"

    def run(self):
        self.running.set()
        for data in self.stream:
            self.output_queue.put(data.decode())
            self.logs.append(data.decode())
            if not self.running.is_set():
                break

    def stop(self):
        self.running.clear()

    def post_join(self):
        for data in self.logs:
            # logger.debug(f"{self.name}: {data}")
            pass


class DockeredWorker:
    def __init__(self, client: docker.DockerClient, name: str):
        self.container = client.containers.run(
            image="chimerapy",
            auto_remove=False,
            stdin_open=True,
            detach=True,
            network_mode="host",  # Not realistic
        )
        self.name = name

        # Create id
        self.id: str = str(uuid.uuid4())

    def connect(self, host, port):

        # Connect worker to Manager through entrypoint
        _, socket = self.container.exec_run(
            cmd=f"cp-worker --id {self.id} --ip {host} --port {port} --name {self.name} --wport 0",
            socket=True,
        )

        try:
            unknown_byte = socket._sock.recv(docker.constants.STREAM_HEADER_SIZE_BYTES)

            buffer_size = 4096  # 4 KiB
            while True:
                part = socket._sock.recv(buffer_size)
                if "connection successful to Manager" in part.decode("utf-8"):
                    break
        except Exception as e:
            raise e

    def shutdown(self):

        # Then wait until the container is done
        self.container.kill()
        self.container.wait()
