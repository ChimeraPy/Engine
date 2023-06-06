import asyncio

import pytest

from .dev_manager_services_group import DevManagerServicesGroup
from ..conftest import TEST_DATA_DIR


@pytest.fixture
async def zeroconf_service():

    # Create the service
    manager_services_group = DevManagerServicesGroup(
        logdir=TEST_DATA_DIR, publisher_port=0
    )

    yield manager_services_group.zeroconf

    await manager_services_group.async_apply("shutdown")


@pytest.mark.asyncio
async def test_enable_and_disable_zeroconf(zeroconf_service):

    zeroconf_service = await zeroconf_service.__anext__()

    assert await zeroconf_service.enable()
    await asyncio.sleep(3)
    assert await zeroconf_service.disable()


@pytest.mark.asyncio
async def test_zeroconf_connect(zeroconf_service, worker):

    zeroconf_service = await zeroconf_service.__anext__()

    assert await zeroconf_service.enable()
    await asyncio.sleep(1)

    assert await worker.async_connect(method="zeroconf")

    await asyncio.sleep(1)
    assert await zeroconf_service.disable()
