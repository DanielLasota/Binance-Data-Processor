import json
import pprint
from queue import Queue
from typing import Any

from binance_archiver.orderbook_level_2_listener.stream_age_enum import StreamAge
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType


class SupervisedQueue:
    def __init__(self, logger, stream_type: StreamType):
        self.logger = logger
        self.queue = Queue()
        self.stream_type = stream_type
        self._last_timestamp_input = {
            StreamAge.NEW: [],
            StreamAge.OLD: []
        }

        choose = {
            StreamType.DIFFERENCE_DEPTH: self._put_difference_depth_message,
            StreamType.TRADE: self._put_trade_message
        }

        self.put = choose.get(stream_type, None)

    def _put_difference_depth_message(self, id_data, message):
        self.queue.put(message)

    def _put_trade_message(self, message):
        self.queue.put(message)

    def get(self) -> Any:
        message = self.queue.get()
        return message

    def get_nowait(self) -> Any:
        message = self.queue.get_nowait()
        return message

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()
