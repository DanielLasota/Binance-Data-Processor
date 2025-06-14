from enum import Enum


class StreamType(Enum):
    UNKNOWN = 'unknown'
    DIFFERENCE_DEPTH_STREAM = 'difference_depth_stream'
    TRADE_STREAM = 'trade_stream'
    DEPTH_SNAPSHOT = 'depth_snapshot'
    FINAL_DEPTH_SNAPSHOT = 'final_depth_snapshot'
    ...

ST_STREAM_CODE = {
    StreamType.UNKNOWN:                 0,
    StreamType.DIFFERENCE_DEPTH_STREAM: 1,
    StreamType.TRADE_STREAM:            2,
    StreamType.DEPTH_SNAPSHOT:          3,
    StreamType.FINAL_DEPTH_SNAPSHOT:    4,
}
