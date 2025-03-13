from __future__ import annotations

import time
from datetime import datetime, timezone


class TimestampsGenerator:
    __slots__ = ()

    @staticmethod
    def get_utc_formatted_timestamp_for_file_name() -> str:
        return datetime.utcnow().strftime("%d-%m-%YT%H-%M-%SZ")

    @staticmethod
    def get_utc_timestamp_epoch_milliseconds() -> int:
        raw_timestamp_of_receive_ns = time.time_ns()
        return (raw_timestamp_of_receive_ns + 500_000) // 1_000_000
