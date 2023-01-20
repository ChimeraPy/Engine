from typing import Any, Literal, Dict
import collections
import pickle
import blosc
import datetime

# Third-party Imports
import numpy as np
import simplejpeg


class DataChunk:
    def __init__(self):
        self._container = collections.defaultdict(dict)

        # Creating mapping from content_type and compression method
        self._content_type_2_serial_mapping = {
            "image": (self._serialize_image, self._deserialize_image),
        }

        # Creating mapping for checking content_type
        self._content_type_2_checks_mapping = {
            "image": self._check_image,
        }

        # Adding default key-value pair
        self._container["meta"] = {
            "value": {
                "ownership": [],
                "created": datetime.datetime.now(),
            },
            "content-type": "meta",
        }

    def __str__(self):
        return f"<DataChunk {list(self._container.keys())}"

    def __eq__(self, other: "DataChunk"):

        # First check data type
        if not isinstance(other, DataChunk):
            return False

        # Check that their records have the same content
        for record_name, record in self._container.items():
            if record_name in other._container:
                other_record = other._container[record_name]
                if record == other_record:
                    return True
                else:
                    return False
            else:
                return False

        return True

    ####################################################################
    # Content Type - Image
    ####################################################################

    def _check_image(self, image: np.ndarray):
        assert isinstance(
            image, np.ndarray
        ), f"Image needs to be an numpy array, currently {type(image)}"
        assert (
            image.dtype == np.uint8
        ), f"Numpy image needs to be np.uint8, currently {image.dtype}"

    def _serialize_image(self, image: np.ndarray):
        return simplejpeg.encode_jpeg(np.ascontiguousarray(image))

    def _deserialize_image(self, image_bytes: bytes):
        return simplejpeg.decode_jpeg(image_bytes)

    ####################################################################
    # (De)Serialization
    ####################################################################

    def _serialize(self) -> bytes:

        # Create serialized container
        s_container = collections.defaultdict(dict)

        # For each entry, serialize and compress based on their content type
        for record_name, record in self._container.items():

            # For certain content-type, additional compression methods
            # are needed
            if record["content-type"] not in ["other", "meta"]:
                s_func = self._content_type_2_serial_mapping[record["content-type"]][0]
                value = s_func(record["value"])
            else:
                value = record["value"]

            s_container[record_name] = {
                "value": value,
                "content-type": record["content-type"],
            }

        # Finished serializing the result
        return blosc.compress(
            pickle.dumps(s_container, protocol=pickle.HIGHEST_PROTOCOL)
        )

    def _deserialize(self, data_bytes: bytes):

        data = pickle.loads(blosc.decompress(data_bytes))

        for record_name, record in data.items():
            # For certain content-type, additional compression methods
            # are needed
            if record["content-type"] not in ["other", "meta"]:
                ds_func = self._content_type_2_serial_mapping[record["content-type"]][1]
                value = ds_func(record["value"])
            else:
                value = record["value"]

            self._container[record_name] = {
                "value": value,
                "content-type": record["content-type"],
            }

    @classmethod
    def from_bytes(cls, data_bytes: bytes):
        instance = cls()
        instance._deserialize(data_bytes)
        return instance

    ####################################################################
    # Front-facing API
    ####################################################################

    def add(
        self, name: str, value: Any, content_type: Literal["image", "other"] = "other"
    ):
        """Add a new record to the DataChunk instance.

        The important parameter here is the content_type, as this will \
        affect the execution speed and real-time ability of a pipeline. \
        As of now, we only have two options: ``image`` and ``other``, as\
        these are the provided serialization methods.

        When sending an image (a numpy array), use the ``image`` option.\
        As for anything else, use the ``other`` option until further \
        notice.

        Args:
            name (str): The name to the record.
            value (Any): The contents to be stored with the name key.
            content_type (Literal["image", "other"]): Specifying the \
                content type to help serialization and compression \
                efficiency.

        """
        # Get the check function (only on specify types of content
        if content_type in ["image"]:
            check_func = self._content_type_2_checks_mapping[content_type]
            check_func(value)

        # Add an entry
        self._container[name] = {"value": value, "content-type": content_type}

    def get(self, name: str) -> Dict[str, Any]:
        """Extract the record given a name.

        Args:
            name (str): The requested key name.

        Returns:
            Dict[str, Any]: Returns a record, stored as a dictionary, \
                with the following attributes: ``value``, ``content-type``\
                and ``ownership``. Mostly you will only need to use \
                ``value``.
        """
        return self._container[name]

    def update(self, name: str, record: Dict[str, Any]):
        """Overwrite record with a new one, deletes previous meta data.

        Args:
            name (str): The name of the record
            record (Dict[str, Any]): The new record to overwrite the \
                pre-existing one.
        """
        self._container[name] = record
