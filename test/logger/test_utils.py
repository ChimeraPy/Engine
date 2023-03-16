import pytest

from chimerapy.logger.utils import bind_pull_socket, connect_push_socket

pytestmark = [pytest.mark.unit]


def test_bind_without_port():
    socket, port = bind_pull_socket()
    assert 60000 > port > 50000
    socket.close()


@pytest.mark.xfail(reason="Port might already be in use")
def test_bind_with_port():
    socket, port = bind_pull_socket(port=55555)
    assert port == 55555
    socket.close()


def test_bind_with_port_out_of_range():
    with pytest.raises(ValueError) as e:
        _, _ = bind_pull_socket(port=633302152145)
        assert (
            f"Port {633302152145} is out of range. Please use a port between 50000 and 60000."
            in e
        )


def test_push_pull_sending():
    socket, port = bind_pull_socket()
    push_socket = connect_push_socket("localhost", port)
    push_socket.send_string("test")
    assert socket.recv_string() == "test"
    socket.close()
    push_socket.close()
