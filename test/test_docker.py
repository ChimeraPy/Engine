import logging
import sys

import pytest

logger = logging.getLogger("chimerapy")

# Resources: https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.Container
# https://stackoverflow.com/questions/61763684/following-the-exec-run-output-from-docker-py-in-realtime


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Docker only supported in Linux in GitHub Actions",
)
def test_get_easy_docker_example_going(docker_client):
    if docker_client:
        output = docker_client.containers.run("ubuntu", "echo $PATH")
        logger.info(output)
    else:
        logger.debug("skipped test")


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Docker only supported in Linux in GitHub Actions",
)
def test_create_container_and_make_it_execute_commands(docker_client):

    if docker_client:

        # Create the docker container
        container = docker_client.containers.run(
            image="ubuntu", auto_remove=False, stdin_open=True, detach=True
        )

        # Start executing commands
        output = container.exec_run(cmd="echo $(find /)")
        logger.info(output)

        output = container.exec_run(cmd="whoami")
        logger.info(output)
    else:
        logger.debug("skipped test")


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Docker only supported in Linux in GitHub Actions",
)
def test_use_custom_docker_image(docker_client):

    if docker_client:

        # Create the docker container
        container = docker_client.containers.run(
            image="chimerapy", auto_remove=False, stdin_open=True, detach=True
        )

        # Start executing commands
        output = container.exec_run(
            cmd="python -c 'import chimerapy; print(chimerapy)'"
        )
        logger.info(output)
    else:
        logger.debug("skipped test")
