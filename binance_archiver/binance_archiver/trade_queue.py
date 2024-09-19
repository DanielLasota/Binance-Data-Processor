from queue import Queue
from typing import Any, final
import threading
import re

from binance_archiver.binance_archiver.market_enum import Market
from binance_archiver.binance_archiver.stream_id import StreamId


class ClassInstancesAmountLimitException(Exception):
    ...


class TradeQueue:
    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 4
    _transaction_signs_compiled_pattern = re.compile(r'"s":"([^"]+)","t":(\d+)')

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if len(cls._instances) >= cls._instances_amount_limit:
                raise ClassInstancesAmountLimitException(f"Cannot create more than {cls._instances_amount_limit} "
                                                         f"instances of TradeQueue")
            instance = super(TradeQueue, cls).__new__(cls)
            cls._instances.append(instance)
            return instance

    @classmethod
    def get_instance_count(cls):
        return len(cls._instances)

    @classmethod
    def clear_instances(cls):
        with cls._lock:
            cls._instances.clear()

    def __init__(self, market: Market):
        self.lock = threading.Lock()
        self._market = market
        self.queue = Queue()
        self.did_websockets_switch_successfully = False
        self.new_stream_listener_id: StreamId | None = None
        self.currently_accepted_stream_id: StreamId | None = None
        self.no_longer_accepted_stream_id: StreamId = StreamId(pairs=[])
        self.last_message_signs: str = ''

    @property
    @final
    def market(self):
        return self._market

    def put_trade_message(
            self,
            stream_listener_id: StreamId,
            message: str,
            timestamp_of_receive: int
    ) -> None:

        with self.lock:
            if stream_listener_id.id == self.no_longer_accepted_stream_id.id:
                return

            if stream_listener_id.id == self.currently_accepted_stream_id.id:
                self.queue.put((message, timestamp_of_receive))
            else:
                self.new_stream_listener_id = stream_listener_id

            current_message_signs = self.get_message_signs(message)

            if current_message_signs == self.last_message_signs:
                self.no_longer_accepted_stream_id = self.currently_accepted_stream_id
                self.currently_accepted_stream_id = self.new_stream_listener_id
                self.new_stream_listener_id = None
                self.did_websockets_switch_successfully = True

            self.last_message_signs = current_message_signs

    @staticmethod
    def get_message_signs(message: str) -> str:
        match = TradeQueue._transaction_signs_compiled_pattern.search(message)
        return '"s":"' + match.group(1) + '","t":' + match.group(2)

    def get(self) -> Any:
        message, received_timestamp = self.queue.get()
        return message, received_timestamp

    def get_nowait(self) -> Any:
        message, received_timestamp = self.queue.get_nowait()
        return message, received_timestamp

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()
