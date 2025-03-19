import uuid
import time

from binance_data_processor.core.exceptions import BadStreamIdParameter


class StreamListenerId:

    __slots__ = [
        'start_timestamp',
        'uuid',
        '_pairs'
    ]

    def __init__(
            self,
            pairs: list[str]
    ):
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
    def id_keys(self) -> tuple[int, uuid.UUID]:
        return self.start_timestamp, self.uuid
