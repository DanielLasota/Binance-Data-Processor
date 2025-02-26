import uuid
import zlib
from queue import Queue
from typing import final, Tuple
import threading
import re
from abc import ABC, abstractmethod

from binance_archiver.exceptions import ClassInstancesAmountLimitException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_listener_id import StreamListenerId


class PutTradeMessageStrategy(ABC):
    @abstractmethod
    def put_trade_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        pass


class ContinuousListeningStrategy(PutTradeMessageStrategy):
    def __init__(
            self,
            context: 'TradeQueue'
    ):
        self.context = context

    def put_trade_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        with self.context.lock:
            stream_listener_id_keys = stream_listener_id.id_keys

            if stream_listener_id_keys == self.context.no_longer_accepted_stream_id_keys:
                return

            if stream_listener_id_keys == self.context.currently_accepted_stream_id_keys:
                message_with_timestamp_of_receive = message[:-1] + f',"_E":{timestamp_of_receive}}}'
                compressed_message = zlib.compress(message_with_timestamp_of_receive.encode('utf-8'), level=9)
                self.context.queue.put(compressed_message)
            else:
                self.context.new_stream_listener_id_keys = stream_listener_id_keys


class SwitchingWebsocketsStrategy(PutTradeMessageStrategy):
    def __init__(
            self,
            context: 'TradeQueue'
    ):
        self.context = context

    def put_trade_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        with self.context.lock:
            stream_listener_id_keys = stream_listener_id.id_keys

            if stream_listener_id_keys == self.context.no_longer_accepted_stream_id_keys:
                return

            if stream_listener_id_keys == self.context.currently_accepted_stream_id_keys:
                message_with_timestamp_of_receive = message[:-1] + f',"_E":{timestamp_of_receive}}}'
                compressed_message = zlib.compress(message_with_timestamp_of_receive.encode('utf-8'), level=9)
                self.context.queue.put(compressed_message)
            else:
                self.context.new_stream_listener_id_keys = stream_listener_id_keys

            current_message_signs = self.context.get_message_signs(message)

            if current_message_signs == self.context.last_message_signs:
                self.context.no_longer_accepted_stream_id_keys = self.context.currently_accepted_stream_id_keys
                self.context.currently_accepted_stream_id_keys = self.context.new_stream_listener_id_keys
                self.context.new_stream_listener_id_keys = None
                self.context.did_websockets_switch_successfully = True
                self.context.set_continuous_listening_mode()

            self.context.last_message_signs = current_message_signs

            del message


class TradeQueue:
    __slots__ = [
        'lock',
        '_market',
        'did_websockets_switch_successfully',
        'new_stream_listener_id_keys',
        'currently_accepted_stream_id_keys',
        'no_longer_accepted_stream_id_keys',
        'last_message_signs',
        '_strategy',
        'queue'
    ]

    _instances = []
    _lock = threading.Lock()
    _INSTANCES_AMOUNT_LIMIT = 3
    _TRANSACTION_SIGNS_COMPILED_PATTERN = re.compile(r'"s":"([^"]+)","t":(\d+)')

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if len(cls._instances) >= cls._INSTANCES_AMOUNT_LIMIT:
                raise ClassInstancesAmountLimitException(
                    f"Cannot create more than {cls._INSTANCES_AMOUNT_LIMIT} instances of TradeQueue")
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

    def __init__(
        self,
        market: Market,
        global_queue: Queue | None = None
    ):
        self.lock = threading.Lock()
        self._market = market

        self.did_websockets_switch_successfully = False
        self.new_stream_listener_id_keys: Tuple[int, uuid.UUID] | None = None
        self.currently_accepted_stream_id_keys: Tuple[int, uuid.UUID] | None = None
        self.no_longer_accepted_stream_id_keys: Tuple[int, uuid.UUID] = StreamListenerId(pairs=[]).id_keys
        self.last_message_signs: str = ''

        self._strategy: PutTradeMessageStrategy = ContinuousListeningStrategy(self)
        self.set_continuous_listening_mode()

        self.queue = Queue() if global_queue is None else global_queue

    @property
    @final
    def market(self):
        return self._market

    def set_continuous_listening_mode(self) -> None:
        self._strategy = ContinuousListeningStrategy(self)

    def set_switching_websockets_mode(self) -> None:
        self._strategy = SwitchingWebsocketsStrategy(self)

    def put_trade_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        self._strategy.put_trade_message(stream_listener_id, message, timestamp_of_receive)

    @staticmethod
    def get_message_signs(message: str) -> str:
        match = TradeQueue._TRANSACTION_SIGNS_COMPILED_PATTERN.search(message)
        return '"s":"' + match.group(1) + '","t":' + match.group(2)

    def get(self) -> any:
        entry = self.queue.get()
        return entry

    def get_nowait(self) -> any:
        entry = self.queue.get_nowait()
        return entry

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()
