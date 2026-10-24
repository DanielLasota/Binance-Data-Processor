import logging
from dataclasses import dataclass


@dataclass(slots=True)
class TimeCfg:
    file_duration_seconds: int = 60*3
    snapshot_interval_seconds: int = 180
    ws_lifetime_seconds: int = 60 * 60 * 6

    def update_interval(
            self,
            time_type: str,
            new_time: int,
            logger: logging.Logger
    ) -> None:

        if time_type not in {'file_duration_seconds', 'snapshot_fetcher_interval_seconds', 'websocket_lifetime_seconds'}:
            raise AttributeError(f"'{time_type}' is not a valid setting name. ")

        if not isinstance(new_time, int):
            raise TypeError(f"new_time must be an integer, got {type(new_time).__name__}.")

        setattr(self, time_type, new_time)
        logger.info(f"Updated '{time_type}' to {new_time} seconds.")
