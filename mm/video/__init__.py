"""
Video Subpackage

This subpackage contains special implementations of ``DataStream`` and 
``Process`` such as ``OfflineVideoDataStream`` and ``ShowVideo`` that
handles video files.

"""

# Subpackage definition
# __package__ = 'video'

# Imports
from .video_data_stream import OfflineVideoDataStream
from .processes import ShowVideo, SaveVideo
