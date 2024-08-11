from queue import Queue
from typing import Any
import threading


class ClassInstancesAmountLimitException(Exception):
    ...


class TradeQueue:
    _instances = []
    _lock = threading.Lock()
    _instances_amount_limit = 6

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

    def __init__(self):
        self.queue = Queue()
        self.unique_elements = set()
        self.did_websockets_switch_successfully = False

    def put_trade_message(self, message: str, timestamp_of_receive) -> None:
        self._put_with_no_repetitions(message, timestamp_of_receive)

    def _put_with_no_repetitions(self, message: str, received_timestamp) -> None:
        if message not in self.unique_elements:
            self.unique_elements.add(message)
            self.queue.put((message, received_timestamp))
        else:
            self.did_websockets_switch_successfully = True

    def get(self) -> Any:
        message, received_timestamp = self.queue.get()
        self.unique_elements.remove(message)
        return message, received_timestamp

    def get_nowait(self) -> Any:
        message, received_timestamp = self.queue.get_nowait()
        self.unique_elements.remove(message)
        return message, received_timestamp

    def clear(self) -> None:
        self.queue.queue.clear()
        self.unique_elements.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()

    def __contains__(self, item: Any) -> bool:
        return item in self.unique_elements
