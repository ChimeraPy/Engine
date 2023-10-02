import json
import tempfile
import time
from pathlib import Path

import pytest

from chimerapy.engine.eventbus import Event, EventBus, make_evented
from chimerapy.engine.manager.events import TagEvent
from chimerapy.engine.manager.session_tag_service import SessionTagService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.states import ManagerState, NodeState, WorkerState


@pytest.fixture(scope="module")
def testbed_setup():
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    state = ManagerState(logdir=Path(tempfile.mkdtemp()))

    make_evented(state, event_bus=eventbus)

    tag_service = SessionTagService(name="tag_service", eventbus=eventbus, state=state)

    return tag_service, state, eventbus


def test_tag_service_tag_error(testbed_setup):
    tag_service, state, bus = testbed_setup
    can_create_tag, reason = tag_service.can_create_tag()

    assert not can_create_tag
    assert reason == "No nodes to tag"


def test_tag_service_tag(testbed_setup):
    # Add workers and nodes to the state
    tag_service, state, bus = testbed_setup

    state.workers["worker1"] = WorkerState(
        name="worker1",
        id="worker1",
        ip="0.0.0.0",
        port=0,
        nodes={"node1": NodeState(name="node1", id="node1", fsm="NULL")},
    )

    can, reason = tag_service.can_create_tag()
    assert not can
    assert reason == "All nodes must be in RECORDING state to add a tag"

    state.workers["worker1"].nodes["node1"].fsm = "RECORDING"
    can, reason = tag_service.can_create_tag()
    assert can
    assert reason is None

    bus.send(Event("start_recording")).result()
    time.sleep(2)
    bus.send(Event("create_tag", TagEvent("A", "tag1"))).result()
    bus.send(Event("update_tag", TagEvent("A", "tag2", "tag2_descr"))).result()
    bus.send(Event("stop_recording")).result()

    with (state.logdir / "session_tags.json").open("r") as f:
        tags = json.load(f)
        assert tags["A"]["name"] == "tag2"
        assert tags["A"]["description"] == "tag2_descr"
        assert tags["A"]["timestamp"] > 2
