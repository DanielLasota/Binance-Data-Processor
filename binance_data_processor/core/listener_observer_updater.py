from __future__ import annotations

import logging
import queue
import threading
from queue import Queue

from binance_data_processor.core.abstract_base_classes import Observer


class ListenerObserverUpdater:

    __slots__ = [
        'logger',
        'observers',
        'global_queue',
        'global_shutdown_flag'
    ]

    def __init__(
            self,
            observers: list[Observer],
            global_queue: Queue,
            global_shutdown_flag: threading.Event
    ) -> None:
        self.logger = logging.getLogger('binance_data_processor')
        self.observers = observers
        self.global_queue = global_queue
        self.global_shutdown_flag = global_shutdown_flag

    def process_global_queue(self) -> None:
        while not self.global_shutdown_flag.is_set():

            if self.global_queue.qsize() > 200:
                # self.logger.warning(f'qsize: {self.global_queue.qsize()}')
                ...

            try:
                message = self.global_queue.get(timeout=1)
                for observer in self.observers:
                    observer.update(message)

            except queue.Empty:
                continue

    def run_whistleblower(self) -> None:
        whistleblower_thread = threading.Thread(target=self.process_global_queue)
        whistleblower_thread.start()
