from queue import Queue
from typing import Any


class TradeQueue:
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
