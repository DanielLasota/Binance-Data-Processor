import logging
import time
import threading
from datetime import datetime, timezone

from binance_archiver.enum_.asset_parameters import AssetParameters


class BlackoutSupervisor:

    __slots__ = [
        'asset_parameters',
        'on_error_callback',
        'max_interval_without_messages_in_seconds',
        'running',
        'lock',
        'thread',
        'logger',
        'last_message_time_epoch_seconds_utc',
    ]

    def __init__(
            self,
            asset_parameters: AssetParameters,
            max_interval_without_messages_in_seconds,
            on_error_callback=None
    ) -> None:
        self.asset_parameters = asset_parameters
        self.max_interval_without_messages_in_seconds = max_interval_without_messages_in_seconds
        self.on_error_callback = on_error_callback
        self.running = False
        self.lock = threading.Lock()
        self.thread = None
        self.logger = logging.getLogger('binance_archiver')
        self.last_message_time_epoch_seconds_utc = ...


    def run(self) -> None:
        self.thread = threading.Thread(
            target=self._monitor_last_message_time,
            name=f'stream_listener blackout supervisor {self.asset_parameters.stream_type} {self.asset_parameters.market}'
        )
        self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())
        self.running = True
        self.thread.start()

    def notify(self) -> None:
        with self.lock:
            self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())

    def _monitor_last_message_time(self) -> None:
        while self.running:
            with self.lock:
                time_since_last_message = (
                        int(datetime.now(timezone.utc).timestamp())
                        - self.last_message_time_epoch_seconds_utc
                )
            if time_since_last_message > self.max_interval_without_messages_in_seconds:
                self.logger.info(
                    f'{self.asset_parameters.market} {self.asset_parameters.stream_type}: '
                    f'Supervisor: No entry for {self.max_interval_without_messages_in_seconds} seconds, '
                    f'sending restart signal.'
                )
                self._send_shutdown_signal()
                self.shutdown_supervisor()
                break

            time.sleep(5)

    def _send_shutdown_signal(self) -> None:
        if self.on_error_callback:
            self.on_error_callback()
        else:
            raise Exception('Error callback not set')

    def shutdown_supervisor(self) -> None:
        self.running = False
