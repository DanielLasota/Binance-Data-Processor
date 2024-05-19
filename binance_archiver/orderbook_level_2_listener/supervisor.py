import time
import threading
from datetime import datetime, timezone
import logging

from binance_archiver.orderbook_level_2_listener.market_enum import Market


class Supervisor:
    def __init__(
            self,
            logger: logging.Logger,
            stream_type: str,
            instrument: str,
            market: Market,
            on_error_callback=None,
    ):
        self.logger = logger
        self.on_error_callback = on_error_callback
        self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())
        self.check_interval_in_seconds = 10
        self.max_interval_without_messages_in_seconds = 10
        self.running = True
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._monitor_last_message_time)
        self.thread.start()
        self.stream_type = stream_type
        self.instrument = instrument
        self.market = market

    def notify(self):
        with self.lock:
            self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())

    def _monitor_last_message_time(self):
        while self.running:
            with self.lock:
                time_since_last_message = (
                        int(datetime.now(timezone.utc).timestamp()) - self.last_message_time_epoch_seconds_utc
                )

            if time_since_last_message > self.max_interval_without_messages_in_seconds:

                self.logger.info(
                    f'Supervisor: {self.stream_type}, {self.instrument}, {self.market} '
                    f'No entry for {self.max_interval_without_messages_in_seconds} seconds, '
                    f'sending restart signal '
                )
                self._stop_algo_on_global_level()
            time.sleep(self.check_interval_in_seconds)

    def _stop_algo_on_global_level(self):
        if self.on_error_callback:
            self.on_error_callback()

        else:
            raise Exception('Error callback not set')

        self._stop()

    def _stop(self):
        self.running = False
