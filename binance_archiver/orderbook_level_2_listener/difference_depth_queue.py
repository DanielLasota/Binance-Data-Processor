import copy
import json
import threading
import time
from queue import Queue
from typing import Any, Dict, final

from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId


class ClassInstancesAmountLimitException(Exception):
    ...


class DifferenceDepthQueue:
    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 6

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

    @property
    @final
    def market(self):
        return self._market

    def _add_to_compare(self, stream_listener_id, message):
        message_dict = json.loads(message)
        id_index = stream_listener_id.id

        if stream_listener_id.id == self.no_longer_accepted_stream_id:
            return

        if id_index not in self._two_last_throws:
            self._two_last_throws[id_index] = []

        if len(self._two_last_throws[id_index]) > 0:
            if message_dict['data']['E'] > self._two_last_throws[id_index][-1]['data']['E'] + 10:
                self._two_last_throws[id_index].clear()
        self._two_last_throws[id_index].append(message_dict)

        amount_of_listened_pairs = stream_listener_id.pairs_amount
        do_they_match = self._compare_two_last_throws(amount_of_listened_pairs, self._two_last_throws)

        if do_they_match is True:
            self.set_new_stream_id_as_currently_accepted()

    def set_new_stream_id_as_currently_accepted(self):
        print('changing')

        self.currently_accepted_stream_id = max(self._two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id = min(self._two_last_throws.keys(), key=lambda x: x[0])

        self._two_last_throws = {}
        self.did_websockets_switch_successfully = True
        print('info from diff depth queue, did_websockets_switch_successfully=True')

    @staticmethod
    def _compare_two_last_throws(amount_of_listened_pairs: int, two_last_throws: Dict) -> bool | None:

        keys = list(two_last_throws.keys())

        if len(keys) < 2:
            return

        if len(two_last_throws[keys[0]]) == len(two_last_throws[keys[1]]) == amount_of_listened_pairs:
            copied_two_last_throws = copy.deepcopy(two_last_throws)

            sorted_and_copied_two_last_throws = DifferenceDepthQueue._sort_entries_by_symbol(copied_two_last_throws)

            for stream_age in sorted_and_copied_two_last_throws:
                for entry in sorted_and_copied_two_last_throws[stream_age]:
                    entry['data'].pop('E', None)

            if sorted_and_copied_two_last_throws[keys[0]] == sorted_and_copied_two_last_throws[keys[1]]:
                return True

        return False

    @staticmethod
    def _sort_entries_by_symbol(two_last_throws: Dict) -> Dict:
        for stream_id in two_last_throws:
            two_last_throws[stream_id].sort(key=lambda entry: entry['data']['s'])
        return two_last_throws

    def put_queue_message(self, message, stream_listener_id: StreamId, timestamp_of_receive: int):
        with self.lock:
            self._add_to_compare(stream_listener_id, message)

            if stream_listener_id.id == self.currently_accepted_stream_id:
                self.queue.put((message, timestamp_of_receive))

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
