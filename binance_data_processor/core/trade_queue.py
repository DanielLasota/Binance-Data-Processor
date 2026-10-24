from collections import deque
from typing import final
import re
from abc import ABC, abstractmethod

from binance_data_processor.core.exceptions import ClassInstancesAmountLimitException
from binance_data_processor.core.shared_ring_buffer import SharedRingBuffer
from binance_data_processor.enums.market_enum import Market


class QueueStrategy(ABC):
    __slots__ = ()

    @abstractmethod
    def put(self, id_: bool, message: str, ts: int) -> None:
        ...


class ContinuousListeningStrategy(QueueStrategy):
    __slots__ = ('context',)

    def __init__(self, context: 'TradeQueue'):
        self.context = context

    def put(self, msg: str, ts: int, cid: bool) -> None:
        if cid == self.context.not_accepted_cid:
            return
        elif cid == self.context.accepted_cid:
            self.context.queue.append(msg[:-1] + f',"_E":{ts}}}') #todo: wyjebac laczenie do data collector update: chyba jednak nie trzeba teraz


class SwitchingWsStrategy(QueueStrategy):
    __slots__ = ('context', )

    _TRANSACTION_SIGNS_COMPILED_PATTERN = re.compile(r'"s":"([^"]+)","t":(\d+)')

    def __init__(self, context: 'TradeQueue'):
        self.context = context

    def put(self, msg: str, ts: int, cid: bool) -> None:
        if cid == self.context.accepted_cid:
            self.context.queue.append(msg[:-1] + f',"_E":{ts}}}')

        current_msg_signs = self.get_msg_signs(msg)

        if current_msg_signs == self.context.last_msg_signs:
            self.context.not_accepted_cid = not self.context.not_accepted_cid
            self.context.accepted_cid = not self.context.accepted_cid
            self.context.did_ws_switch = True
            self.context.set_continuous_listening_mode()

        self.context.last_msg_signs = current_msg_signs

        del msg

    @staticmethod
    def get_msg_signs(msg: str) -> str:
        match = SwitchingWsStrategy._TRANSACTION_SIGNS_COMPILED_PATTERN.search(msg)
        return '"s":"' + match.group(1) + '","t":' + match.group(2)

class TradeQueue:
    __slots__ = (
        'queue',
        'srb',
        'accepted_cid',
        'not_accepted_cid',
        'pairs_amount',
        'did_ws_switch',
        'last_msg_signs',
        '_strategy',
        '_market'
    )

    _instances = []
    _INSTANCES_AMOUNT_LIMIT = 3

    def __new__(cls, *args, **kwargs):
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
        cls._instances.clear()

    def __init__(self, market: Market, pairs_n: int, global_queue = None):
        self.queue = deque()
        self.srb = SharedRingBuffer.create(f'client_to_collector_tq_{market.name}', 1 << 22) if not global_queue else global_queue
        self.accepted_cid: bool = False
        self.not_accepted_cid: bool = True
        self.pairs_amount = pairs_n
        self.did_ws_switch = False
        self.last_msg_signs: str = ''
        self._strategy: SwitchingWsStrategy | ContinuousListeningStrategy = ContinuousListeningStrategy(self)
        self._market = market

    @property
    @final
    def market(self):
        return self._market

    def update_pairs_n(self, n: int) -> None:
        ...
        #todo
        # if isinstance(self._strategy, SwitchingWsStrategy):
            # self._strategy.update_pairs_amount(val)

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

    def clear(self) -> None:
        self.queue.clear()

    def close(self):
        self.srb.close()
        self.srb.unlink()
        del self.srb
        del self.queue
