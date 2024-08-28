import uuid
import time
from typing import Tuple


class StreamId:
    def __init__(self):
        self.start_timestamp = time.time_ns()
        self.uuid = uuid.uuid4()
        self.pairs_amount = None

    @property
    def id(self) -> Tuple[int, uuid.UUID]:
        return self.start_timestamp, self.uuid
