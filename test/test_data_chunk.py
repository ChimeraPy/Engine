import json

import pytest
from pytest_lazyfixture import lazy_fixture

import numpy as np
import chimerapy.engine as cpe


@pytest.fixture
def video_data_chunk():
    data_chunk = cpe.DataChunk()
    data_chunk.add(
        "image",
        (np.random.rand(10, 10, 3) * 255).astype(np.uint8),
        content_type="image",
    )
    return data_chunk


@pytest.fixture
def images_data_chunk():
    data_chunk = cpe.DataChunk()
    data_chunk.add(
        "images",
        [(np.random.rand(10, 10, 3) * 255).astype(np.uint8) for _ in range(10)],
    )
    return data_chunk


@pytest.fixture
def grey_images_data_chunk():
    data_chunk = cpe.DataChunk()
    data_chunk.add(
        "images",
        [(np.random.rand(10, 10) * 255).astype(np.uint8) for _ in range(10)],
    )
    return data_chunk


@pytest.mark.parametrize(
    "data_chunk",
    [
        (lazy_fixture("video_data_chunk")),
        (lazy_fixture("images_data_chunk")),
        (lazy_fixture("grey_images_data_chunk")),
    ],
)
def test_jsonify_data_chunk(data_chunk):
    json_data = json.dumps(data_chunk.to_json())
    new_data_chunk = cpe.DataChunk.from_json(json.loads(json_data))
    assert data_chunk == new_data_chunk
