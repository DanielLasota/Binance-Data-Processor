import json
import threading
from queue import Queue
from typing import Any, Dict, final
from collections import deque
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
import pprint

class ClassInstancesAmountLimitException(Exception):
    ...


class DifferenceDepthQueue:
    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 4

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if len(cls._instances) >= cls._instances_amount_limit:
                raise ClassInstancesAmountLimitException(f"Cannot create more than {cls._instances_amount_limit} "
                                                         f"instances of DifferenceDepthQueue")
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

    def __init__(self, market: Market):
        self._market = market
        self.queue = Queue()
        self.lock = threading.Lock()
        self.currently_accepted_stream_id = None
        self.no_longer_accepted_stream_id = None
        self.did_websockets_switch_successfully = False
        self._two_last_throws = {}
        # self.are_we_currently_changing: bool = False

    @property
    @final
    def market(self):
        return self._market

    def put_queue_message(self, message: str, stream_listener_id: StreamId, timestamp_of_receive: int) -> None:
        with self.lock:
            if stream_listener_id.id == self.no_longer_accepted_stream_id:
                return

            if stream_listener_id.id == self.currently_accepted_stream_id:
                self.queue.put((message, timestamp_of_receive))

            self._append_message_to_compare_structure(stream_listener_id, message)

            do_throws_match = self._do_last_two_throws_match(stream_listener_id.pairs_amount, self._two_last_throws)

            if do_throws_match is True:
                self.set_new_stream_id_as_currently_accepted()

    def _append_message_to_compare_structure(self, stream_listener_id: StreamId, message: str) -> None:
        id_index = stream_listener_id.id

        message_dict = json.loads(message)
        message_dict['data'].pop('E', None)
        message_str: str = json.dumps(message_dict, sort_keys=True)

        message_list = self._two_last_throws.setdefault(id_index, deque(maxlen=stream_listener_id.pairs_amount))
        message_list.append(message_str)

    @staticmethod
    def _do_last_two_throws_match(amount_of_listened_pairs: int, two_last_throws: Dict) -> bool:
        keys = list(two_last_throws.keys())

        if len(keys) < 2:
            return False

        if len(two_last_throws[keys[0]]) == len(two_last_throws[keys[1]]) == amount_of_listened_pairs:

            last_throw_streams = {json.loads(entry)['stream'] for entry in two_last_throws[keys[0]]}
            second_last_throw_streams = {json.loads(entry)['stream'] for entry in two_last_throws[keys[1]]}

            if len(last_throw_streams) != amount_of_listened_pairs or len(second_last_throw_streams) != amount_of_listened_pairs:
                return False

            match = two_last_throws[keys[0]] == two_last_throws[keys[1]]

            if match is True:
                pprint.pprint(two_last_throws)
                return True

        return False

    @staticmethod
    def _sort_entries_by_symbol(two_last_throws: Dict) -> Dict:
        for stream_id in two_last_throws:
            two_last_throws[stream_id].sort(key=lambda entry: entry['data']['s'])
        return two_last_throws

    def set_new_stream_id_as_currently_accepted(self):
        self.currently_accepted_stream_id = max(self._two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id = min(self._two_last_throws.keys(), key=lambda x: x[0])

        print('>>>>>>>>>>>>>>>>>>>>>>changin')

        self._two_last_throws = {}
        self.did_websockets_switch_successfully = True

    def get(self) -> Any:
        entry = self.queue.get()
        return entry

    def get_nowait(self) -> Any:
        entry = self.queue.get_nowait()
        return entry

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()
