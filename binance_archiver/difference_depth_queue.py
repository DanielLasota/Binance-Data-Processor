import re
import uuid
from queue import Queue
from collections import deque
from typing import final, Tuple
import threading
from abc import ABC, abstractmethod

import orjson

from binance_archiver.exceptions import ClassInstancesAmountLimitException
from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_listener_id import StreamListenerId


class PutDepthMessageStrategy(ABC):
    @abstractmethod
    def put_difference_depth_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        ...


class ContinuousListeningStrategy(PutDepthMessageStrategy):
    def __init__(
            self,
            context: 'DifferenceDepthQueue'
    ):
        self.context = context

    def put_difference_depth_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        with self.context.lock:
            if stream_listener_id.id_keys == self.context.currently_accepted_stream_id_keys:
                message_with_timestamp_of_receive = message[:-1] + f',"_E":{timestamp_of_receive}}}'
                self.context.queue.put(message_with_timestamp_of_receive)


class SwitchingWebsocketsStrategy(PutDepthMessageStrategy):
    def __init__(
            self,
            context: 'DifferenceDepthQueue'
    ):
        self.context = context

    def put_difference_depth_message(
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
                self.context.queue.put(message_with_timestamp_of_receive)

            self.context.append_message_to_compare_structure(stream_listener_id, message)

            do_throws_match = self.context.do_last_two_throws_match(
                stream_listener_id.pairs_amount,
                self.context.two_last_throws
            )

            if do_throws_match:
                self.context.set_new_stream_id_as_currently_accepted()

            del message


class DifferenceDepthQueue:
    __slots__ = [
        '_market',
        'lock',
        'currently_accepted_stream_id_keys',
        'no_longer_accepted_stream_id_keys',
        'did_websockets_switch_successfully',
        'two_last_throws',
        '_strategy',
        'queue'
    ]

    _instances = []
    _lock = threading.Lock()
    _INSTANCES_AMOUNT_LIMIT = 3
    _EVENT_TIMESTAMP_COMPILED_PATTERN = re.compile(r'"E":\d+,')

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if len(cls._instances) >= cls._INSTANCES_AMOUNT_LIMIT:
                raise ClassInstancesAmountLimitException(
                    f"Cannot create more than {cls._INSTANCES_AMOUNT_LIMIT} instances of DifferenceDepthQueue")
            instance = super(DifferenceDepthQueue, cls).__new__(cls)
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
        self._market = market
        self.lock = threading.Lock()
        self.currently_accepted_stream_id_keys: Tuple[int, uuid.UUID] | None = None
        self.no_longer_accepted_stream_id_keys: Tuple[int, uuid.UUID] = StreamListenerId(pairs=[]).id_keys
        self.did_websockets_switch_successfully = False
        self.two_last_throws = {}

        self._strategy: PutDepthMessageStrategy = ContinuousListeningStrategy(self)
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

    def put_difference_depth_message(
        self,
        stream_listener_id: StreamListenerId,
        message: str,
        timestamp_of_receive: int
    ) -> None:
        self._strategy.put_difference_depth_message(stream_listener_id, message, timestamp_of_receive)

    def append_message_to_compare_structure(
        self,
        stream_listener_id: StreamListenerId,
        message: str
    ) -> None:
        id_index = stream_listener_id.id_keys
        message_str = self._remove_event_timestamp(message)
        message_list = self.two_last_throws.setdefault(id_index, deque(maxlen=stream_listener_id.pairs_amount))
        message_list.append(message_str)

    @staticmethod
    def do_last_two_throws_match(
        amount_of_listened_pairs: int,
        two_last_throws: dict
    ) -> bool:
        if len(two_last_throws) < 2:
            return False

        keys = list(two_last_throws.keys())
        last_throw = two_last_throws[keys[0]]
        second_last_throw = two_last_throws[keys[1]]

        if len(last_throw) != amount_of_listened_pairs or len(second_last_throw) != amount_of_listened_pairs:
            return False

        last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in last_throw}
        second_last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in second_last_throw}

        if (len(last_throw_streams_set) != amount_of_listened_pairs
                or len(second_last_throw_streams_set) != amount_of_listened_pairs):
            return False

        return last_throw == second_last_throw

    def set_new_stream_id_as_currently_accepted(self) -> None:
        self.currently_accepted_stream_id_keys = max(self.two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id_keys = min(self.two_last_throws.keys(), key=lambda x: x[0])

        self.two_last_throws = {}
        self.did_websockets_switch_successfully = True

        self.set_continuous_listening_mode()

    @staticmethod
    def _remove_event_timestamp(message: str) -> str:
        return DifferenceDepthQueue._EVENT_TIMESTAMP_COMPILED_PATTERN.sub('', message)

    def update_deque_max_len(self, new_max_len: int) -> None:
        for id_index in self.two_last_throws:
            existing_deque = self.two_last_throws[id_index]
            updated_deque = deque(existing_deque, maxlen=new_max_len)
            self.two_last_throws[id_index] = updated_deque

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
