import uuid
import time
from typing import Tuple, List

from binance_archiver.exceptions import BadStreamIdParameter


class StreamId:
    def __init__(self, pairs: List[str]):
        self.start_timestamp = time.time_ns()
        self.uuid = uuid.uuid4()
        self._pairs = pairs

    @property
    def pairs_amount(self) -> int:
        amount_of_listened_pairs = len(self._pairs)
        if amount_of_listened_pairs is None or amount_of_listened_pairs == 0:
            raise BadStreamIdParameter('stream listener id amount_of_listened_pairs is None or 0')
        return amount_of_listened_pairs

    @property
    def id(self) -> Tuple[int, uuid.UUID]:
        return self.start_timestamp, self.uuid
