import uuid
import time
from typing import Tuple


class StreamId:
    def __init__(self, pairs):
        self.start_timestamp = time.time_ns()
        self.uuid = uuid.uuid4()
        self._pairs = pairs

    @property
    def pairs_amount(self) -> int:
        return len(self._pairs)

    @property
    def id(self) -> Tuple[int, uuid.UUID]:
        return self.start_timestamp, self.uuid
