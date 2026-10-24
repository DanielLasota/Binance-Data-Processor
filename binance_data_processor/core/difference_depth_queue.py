import re
from collections import deque
from typing import final
from abc import ABC, abstractmethod
import orjson

from binance_data_processor.core.exceptions import ClassInstancesAmountLimitException
from binance_data_processor.core.shared_ring_buffer import SharedRingBuffer
from binance_data_processor.enums.market_enum import Market


class QueueStrategy(ABC):
    __slots__ = ()

    @abstractmethod
    def put(self, id_: bool, msg: str, timestamp_of_receive: int) -> None:
        ...


class ContinuousListeningStrategy(QueueStrategy):
    __slots__ = ('context',)

    def __init__(self, context: 'DifferenceDepthQueue'):
        self.context = context

    def put(self, msg: str, ts: int, cid: bool) -> None:
        if cid != self.context.accepted_cid:
            return

        self.context.queue.append(msg[:-1] + f',"_E":{ts}}}')

class SwitchingWsStrategy(QueueStrategy):
    __slots__ = ('context', 'two_last_throws')

    _EVENT_TIMESTAMP_COMPILED_PATTERN = re.compile(r'"E":\d+,')

    def __init__(self, context: 'DifferenceDepthQueue'):
        self.context = context
        self.two_last_throws = {}

    def put(self, msg: str, ts: int, cid: bool) -> None:

        if cid == self.context.accepted_cid:
            self.context.queue.append(msg[:-1] + f',"_E":{ts}}}')

        self.append_msg_to_compare_struct(cid, self.context.pairs_amount, msg)

        if self.do_last_two_throws_match(self.context.pairs_amount, self.two_last_throws):
            self.set_new_client_id_as_accepted()

        del msg

    def append_msg_to_compare_struct(self, id_: bool, pairs_amount: int, msg: str) -> None:
        msg_str = self.del_event_ts(msg)
        self.two_last_throws.setdefault(id_, deque(maxlen=pairs_amount)).append(msg_str)

    @staticmethod
    def del_event_ts(message: str) -> str:
        return SwitchingWsStrategy._EVENT_TIMESTAMP_COMPILED_PATTERN.sub('', message)

    @staticmethod
    def do_last_two_throws_match(pairs_amount: int, two_last_throws: dict) -> bool:
        if len(two_last_throws) < 2:
            return False

        keys = list(two_last_throws.keys())
        last_throw = two_last_throws[keys[0]]
        second_last_throw = two_last_throws[keys[1]]

        if len(last_throw) != pairs_amount or len(second_last_throw) != pairs_amount:
            return False

        last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in last_throw}
        second_last_throw_streams_set = {orjson.loads(entry)['stream'] for entry in second_last_throw}

        if len(last_throw_streams_set) != pairs_amount or len(second_last_throw_streams_set) != pairs_amount:
            return False

        return last_throw == second_last_throw

    def set_new_client_id_as_accepted(self) -> None:
        self.context.accepted_cid = not self.context.accepted_cid
        self.context.not_accepted_cid = not self.context.not_accepted_cid

        self.two_last_throws = {}
        self.context.did_ws_switch = True
        self.context.set_continuous_listening_mode()

    def update_pairs_amount(self, val: int) -> None:
        for key in self.two_last_throws:
            self.two_last_throws[key] = deque(self.two_last_throws[key], maxlen=val)


class DifferenceDepthQueue:
    __slots__ = (
        'queue',
        'srb',
        'accepted_cid',
        'not_accepted_cid',
        'pairs_amount',
        'did_ws_switch',
        '_strategy',
        '_market',
    )

    _instances = []
    _INSTANCES_AMOUNT_LIMIT = 3

    def __new__(cls, *args, **kwargs):
        if len(cls._instances) >= cls._INSTANCES_AMOUNT_LIMIT:
            raise ClassInstancesAmountLimitException(f"Cannot create more than {cls._INSTANCES_AMOUNT_LIMIT} instances of DifferenceDepthQueue")
        instance = super(DifferenceDepthQueue, cls).__new__(cls)
        cls._instances.append(instance)
        return instance

    @classmethod
    def get_instance_count(cls):
        return len(cls._instances)

    @classmethod
    def clear_instances(cls):
        cls._instances.clear()

    def __init__(self, market: Market, pairs_n: int, global_queue = None):
        self.queue = deque()
        self.srb = SharedRingBuffer.create(f'client_to_collector_dq_{market.name}', 1 << 22) if not global_queue else global_queue
        self.accepted_cid: bool = False
        self.not_accepted_cid: bool = True
        self.pairs_amount = pairs_n
        self.did_ws_switch = False
        self._market = market
        self._strategy: SwitchingWsStrategy | ContinuousListeningStrategy = ContinuousListeningStrategy(self)

    @property
    @final
    def market(self):
        return self._market

    def update_pairs_n(self, n: int) -> None:
        #todo
        if isinstance(self._strategy, SwitchingWsStrategy):
            self._strategy.update_pairs_amount(n)

    def set_continuous_listening_mode(self) -> None:
        self._strategy = ContinuousListeningStrategy(self)

    def set_switching_ws_mode(self) -> None:
        self._strategy = SwitchingWsStrategy(self)

    def put(self, msg: str, ts: int, cid: bool) -> None:
        self._strategy.put(msg, ts, cid)

    def get(self) -> str:
        return self.queue.popleft()

    def get_nowait(self) -> str:
        return self.queue.popleft()

    def empty(self) -> bool:
        return not self.queue

    def qsize(self) -> int:
        return len(self.queue)

    def close(self):
        self.srb.close()
        self.srb.unlink()
        del self.srb
        del self.queue