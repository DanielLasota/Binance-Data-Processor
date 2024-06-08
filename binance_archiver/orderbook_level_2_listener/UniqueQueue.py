from queue import Queue
from typing import Any


class UniqueQueue:
    def __init__(self):
        self.queue = Queue()
        self.unique_elements = set()

    def put_with_no_repetitions(self, item: Any) -> bool:
        if item not in self.unique_elements:
            self.unique_elements.add(item)
            self.queue.put(item)
            return True
        return False

    def get(self) -> Any:
        item = self.queue.get()
        self.unique_elements.remove(item)
        return item

    def get_nowait(self) -> Any:
        item = self.queue.get_nowait()
        self.unique_elements.remove(item)
        return item

    def clear(self) -> None:
        self.queue.queue.clear()
        self.unique_elements.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def contains(self, item: Any) -> bool:
        return item in self.unique_elements

    def qsize(self) -> int:
        return self.queue.qsize()

    def __contains__(self, item: Any) -> bool:
        return item in self.unique_elements
