import logging
import time
import threading
from datetime import datetime, timezone

class BlackoutSupervisor:

    __slots__ = [
        'max_interval_without_messages_in_seconds',
        'on_error_callback',
        'logger',
        'thread',
        '_running',
        '_lock',
        '_last_message_time_epoch_seconds_utc'
    ]

    def __init__(
            self,
            max_interval_without_messages_in_seconds: int,
            on_error_callback=None,
    ) -> None:
        self.max_interval_without_messages_in_seconds = max_interval_without_messages_in_seconds
        self.on_error_callback = on_error_callback
        self.logger = logging.getLogger('binance_data_sink')
        self.thread = None

        self._running = False
        self._lock = threading.Lock()
        self._last_message_time_epoch_seconds_utc = ...


    def run(self) -> None:
        self.thread = threading.Thread(
            target=self._monitor_last_message_time,
            name=f'blackout_supervisor'
        )
        self._last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())
        self._running = True
        self.thread.start()

    def notify(self) -> None:
        with self._lock:
            self._last_message_time_epoch_seconds_utc = int(datetime.now(timezone.utc).timestamp())

    def _monitor_last_message_time(self) -> None:
        while self._running:
            with self._lock:
                now_epoch = datetime.now(timezone.utc).timestamp()
                time_since_last_message = now_epoch - self._last_message_time_epoch_seconds_utc
            if time_since_last_message > self.max_interval_without_messages_in_seconds:
                self.shutdown_supervisor()
                self._send_shutdown_signal()
                break
            time.sleep(5)

    def _send_shutdown_signal(self) -> None:
        self.logger.info('Blackout Supervisor detected too long blackout. Callback invocation')
        self.on_error_callback()

    def shutdown_supervisor(self) -> None:
        self._running = False
