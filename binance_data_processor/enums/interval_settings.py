import logging
from dataclasses import dataclass


@dataclass(slots=True)
class IntervalSettings:
    file_duration_seconds: int = 60*5
    snapshot_fetcher_interval_seconds: int = 60
    websocket_life_time_seconds: int = 60*60*6

    def validate(self):
        if self.file_duration_seconds <= 0:
            raise ValueError("file_duration_seconds must be greater than 0")
        if self.snapshot_fetcher_interval_seconds <= 0:
            raise ValueError("snapshot_fetcher_interval_seconds must be greater than 0")
        if self.websocket_life_time_seconds <= 0:
            raise ValueError("websocket_life_time_seconds must be greater than 0")

    def update_interval(
            self,
            setting_name: str,
            new_time: int,
            logger: logging.Logger
    ) -> None:

        valid_settings = {
            'file_duration_seconds',
            'snapshot_fetcher_interval_seconds',
            'websocket_life_time_seconds'
        }

        if setting_name not in valid_settings:
            raise AttributeError(f"'{setting_name}' is not a valid setting name. "
                                 f"Valid settings are: {', '.join(valid_settings)}.")

        if not isinstance(new_time, int):
            raise TypeError(f"new_time must be an integer, got {type(new_time).__name__}.")

        if new_time <= 0:
            raise ValueError("new_time must be greater than 0.")

        setattr(self, setting_name, new_time)
        logger.info(f"Updated '{setting_name}' to {new_time} seconds.")

        self.validate()
