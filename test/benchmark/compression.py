import pickle
import time

import numpy as np

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")

import lz4.block
import blosc


def lz4_com_op(payload):
    return lz4.block.compress(payload)


def lz4_dec_op(c_payload):
    return lz4.block.decompress(c_payload)


def blosc_com_op(payload):
    return blosc.compress(payload)


def blosc_dec_op(c_payload):
    return blosc.decompress(c_payload)


def payload_compression_performance():

    operations = {
        "lz4": (lz4_com_op, lz4_dec_op),
        "blosc": (blosc_com_op, blosc_dec_op),
    }

    # Create msg data
    img = np.random.rand(1920, 1080, 3)
    payload = pickle.dumps({"img": img})

    for name, (com_fun, dec_fun) in operations.items():

        # Performance information
        pre_deltas = []
        pos_deltas = []
        compression_factor = 1

        for i in range(100):
            tic = time.perf_counter()
            c_payload = com_fun(payload)

            tac = time.perf_counter()
            new_payload = dec_fun(c_payload)

            toc = time.perf_counter()
            # assert isinstance(new_payload['img'], np.ndarray)

            pre_deltas.append(tac - tic)
            pos_deltas.append(toc - tac)

        compression_factor = len(c_payload) / len(payload)

        logger.info(
            f"{name} -> Serial: {1/(sum(pre_deltas)/len(pre_deltas)):.2f}, Deserial: {1/(sum(pos_deltas)/len(pos_deltas)):.2f}, Compression Ratio: {compression_factor:.2f}"
        )


if __name__ == "__main__":
    payload_compression_performance()
