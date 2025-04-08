from enum import Enum


class StreamType(Enum):
    DIFFERENCE_DEPTH_STREAM = 'difference_depth_stream'
    TRADE_STREAM = 'trade_stream'
    DEPTH_SNAPSHOT = 'depth_snapshot'
    ...
