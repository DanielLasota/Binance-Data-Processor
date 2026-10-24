import multiprocessing as mp
from dataclasses import dataclass
from typing import Any

from binance_data_processor.enums.stream_type_enum import StreamType


@dataclass(slots=True)
class QueueFlags:
    did_ws_switch: Any
    set_switching_ws_mode: Any


class QueueFlagsPool:
    __slots__ = ('_flags',)

    def __init__(self, cfg):
        self._flags = {
            (market, stream_type): QueueFlags(
                did_ws_switch=mp.Event(),
                set_switching_ws_mode=mp.Event(),
            )
            for market in cfg.instruments.dict.keys()
            for stream_type in [
                StreamType.DIFFERENCE_DEPTH_STREAM,
                StreamType.TRADE_STREAM,
            ]
        }

    def get_flags(self, market, stream_type) -> QueueFlags:
        return self._flags[(market, stream_type)]
