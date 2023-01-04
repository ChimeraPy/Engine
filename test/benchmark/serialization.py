import time
import json

import numpy as np

import nujson
import orjson
import cloudpickle
import pickle
import msgpack
import msgpack_numpy as m

m.patch()

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")


def pickle_se_op(payload):
    return pickle.dumps(payload)


def pickle_de_op(b_payload):
    return pickle.loads(b_payload)


def pickle5_se_op(payload):
    return pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)


def pickle5_de_op(b_payload):
    return pickle.loads(b_payload)


def nujson_se_op(payload):
    return nujson.dumps(payload)


def nujson_de_op(b_payload):
    return nujson.loads(b_payload)


def orjson_se_op(payload):
    return orjson.dumps(payload)


def orjson_de_op(b_payload):
    return orjson.loads(b_payload)


def cloudpickle_se_op(payload):
    return cloudpickle.dumps(payload)


def cloudpickle_de_op(b_payload):
    return cloudpickle.loads(b_payload)


def msgpack_se_op(payload):
    return msgpack.packb(payload)


def msgpack_de_op(b_payload):
    return msgpack.unpackb(b_payload)


def payload_serial_deserial_performance():

    operations = {
        "pickle": (pickle_se_op, pickle_de_op),
        "pickle5": (pickle5_se_op, pickle5_de_op),
        # 'nujson': (nujson_se_op, nujson_de_op), # Too slow
        # 'orjson': (orjson_se_op, orjson_de_op), # Doesn't work with Numpy Arrays
        "cloudpickle": (cloudpickle_se_op, cloudpickle_de_op),
        "msgpack": (msgpack_se_op, msgpack_de_op),
    }

    # Create msg data
    img = np.random.rand(1920, 1080, 3)
    payload = {"img": img}

    for name, (ser_fun, des_fun) in operations.items():

        # Performance information
        pre_deltas = []
        pos_deltas = []

        for i in range(100):
            tic = time.perf_counter()
            b_payload = ser_fun(payload)

            tac = time.perf_counter()
            assert type(b_payload) == bytes, f"{name}, {b_payload}"
            new_payload = des_fun(b_payload)

            toc = time.perf_counter()
            # assert isinstance(new_payload['img'], np.ndarray)

            pre_deltas.append(tac - tic)
            pos_deltas.append(toc - tac)

        # logger.info(f"{name} -> Min: {min(pre_deltas):.5f}, Average: {sum(pre_deltas)/len(pre_deltas):.5f}, Max: {max(pre_deltas):.5f}")
        # logger.info(f"{name} -> Min: {min(pos_deltas):.5f}, Average: {sum(pos_deltas)/len(pos_deltas):.5f}, Max: {max(pos_deltas):.5f}")
        logger.info(
            f"{name} -> Serial: {1/(sum(pre_deltas)/len(pre_deltas)):.2f}, Deserial: {1/(sum(pos_deltas)/len(pos_deltas)):.2f}"
        )

    # An acaptable delay is < 0.01 seconds
    # assert sum(deltas)/len(deltas) < 0.01


if __name__ == "__main__":
    payload_serial_deserial_performance()
