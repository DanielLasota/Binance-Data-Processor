import time
import threading
from datetime import datetime, timezone
import logging

from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType


class Supervisor:
    def __init__(
            self,
            logger: logging.Logger,
            stream_type: StreamType,
            market: Market,
            on_error_callback=None
    ) -> None:
        self.logger = logger
        self.stream_type = stream_type
        self.market = market
        self.on_error_callback = on_error_callback
        self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())
        self.check_interval_in_seconds = 10
        self.max_interval_without_messages_in_seconds = 10
        self.running = True
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._monitor_last_message_time)
        self.thread.start()

    def notify(self):
        with self.lock:
            self.last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())

    def _monitor_last_message_time(self):
        while self.running:
            with self.lock:
                time_since_last_message = (int(datetime.now(timezone.utc).timestamp())
                                           - self.last_message_time_epoch_seconds_utc)
            if time_since_last_message > self.max_interval_without_messages_in_seconds:
                self.logger.info(
                    f'{self.market} {self.stream_type}: '
                    f'Supervisor: No entry for {self.max_interval_without_messages_in_seconds} seconds, '
                    f'sending restart signal.'
                )
                self._send_shutdown_signal()
            time.sleep(self.check_interval_in_seconds)

    def _send_shutdown_signal(self):
        if self.on_error_callback:
            self.on_error_callback()
        else:
            raise Exception('Error callback not set')
        self._stop_as_we_reached_no_signal()

    def _stop_as_we_reached_no_signal(self):
        self.running = False

    def shutdown_supervisor(self):
        self.running = False
