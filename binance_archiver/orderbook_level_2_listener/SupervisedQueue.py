import copy
import json
import pprint
import threading
from queue import Queue
from typing import Any, Dict

from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType


class SupervisedQueue:
    def __init__(self, logger, stream_type: StreamType):
        self.logger = logger
        self.queue = Queue()
        self.stream_type = stream_type
        self.lock = threading.Lock()
        self.currently_accepted_stream_id = None
        self.no_longer_accepted_stream_id = None

        choose = {
            StreamType.DIFFERENCE_DEPTH: self.put_difference_depth_message,
            StreamType.TRADE: self.put_trade_message
        }

        self.put = choose.get(stream_type, None)

        if stream_type == StreamType.DIFFERENCE_DEPTH:
            self.did_websockets_switch_successfully = False

            self._two_last_throws = {}

    def _add_to_compare(self, stream_listener_id, message):
        message_dict = json.loads(message)
        id_index = stream_listener_id.id

        if stream_listener_id.id == self.no_longer_accepted_stream_id:
            print('returning as it is no longer accepted')
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

        pprint.pprint(self._two_last_throws)

        self.currently_accepted_stream_id = max(self._two_last_throws.keys(), key=lambda x: x[0])
        self.no_longer_accepted_stream_id = min(self._two_last_throws.keys(), key=lambda x: x[0])

        print(f'self.currently_accepted_stream_id {self.currently_accepted_stream_id}')
        print(f'self.no_longer_accepted_stream_id {self.no_longer_accepted_stream_id}')

        self._two_last_throws = {}
        self.did_websockets_switch_successfully = True

    @staticmethod
    def _compare_two_last_throws(amount_of_listened_pairs: int, two_last_throws: Dict) -> bool | None:

        keys = list(two_last_throws.keys())

        if len(keys) < 2:
            return

        if len(two_last_throws[keys[0]]) == len(two_last_throws[keys[1]]) == amount_of_listened_pairs:

            copied_two_last_throws = copy.deepcopy(two_last_throws)

            sorted_and_copied_two_last_throws = SupervisedQueue._sort_entries_by_symbol(copied_two_last_throws)

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

    def put_difference_depth_message(self, stream_listener_id: StreamId, message):

        with self.lock:
            self._add_to_compare(stream_listener_id, message)

            if stream_listener_id.id == self.currently_accepted_stream_id:
                self.queue.put(message)

    def put_trade_message(self, message):
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
