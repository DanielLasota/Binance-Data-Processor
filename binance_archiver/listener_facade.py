from __future__ import annotations

import pprint
import time
import threading

from binance_archiver.logo import binance_archiver_logo
from . import DataSinkConfig
from .listener_observer_updater import ListenerObserverUpdater
from .queue_pool import ListenerQueuePool
from .setup_logger import setup_logger
from .abstract_base_classes import Subject, Observer
from .snapshot_manager import ListenerDepthSnapshotStrategy, DepthSnapshotService
from .stream_service import StreamService

__all__ = [
    'launch_data_listener',
    'BinanceDataListener'
]


def launch_data_listener(
        data_sink_config: DataSinkConfig = DataSinkConfig(),
        observers: list[object] = None
) -> BinanceDataListener:

    listener_facade = BinanceDataListener(
        data_sink_config=data_sink_config,
        init_observers=observers
    )

    listener_facade.run()
    return listener_facade

class BinanceDataListener(Subject):

    __slots__ = [
        'data_sink_config',
        'logger',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        '_observers',
        'listener_observer_updater',
        'listener_snapshot_service'
    ]

    def __init__(
            self,
            data_sink_config: DataSinkConfig,
            init_observers: list[Observer] | None = None
    ) -> None:

        self.data_sink_config = data_sink_config

        self.logger = setup_logger(should_dump_logs=True)
        self.logger.info("\n%s", binance_archiver_logo)
        self.logger.info("Configuration:\n%s", pprint.pformat(data_sink_config, indent=1))

        self.global_shutdown_flag = threading.Event()

        self.queue_pool = ListenerQueuePool()

        self._observers = init_observers if init_observers is not None else []

        self.stream_service = StreamService(
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag,
            data_sink_config=data_sink_config
        )

        self.listener_observer_updater = ListenerObserverUpdater(
            observers=self._observers,
            global_queue=self.queue_pool.global_queue,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self.listener_snapshot_service = DepthSnapshotService(
            snapshot_strategy=ListenerDepthSnapshotStrategy(global_queue=self.queue_pool.global_queue),
            data_sink_config=data_sink_config,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self, message) -> None:
        for observer in self._observers:
            observer.update(message)

    def run(self) -> None:

        self.stream_service.run()

        self.listener_observer_updater.run_whistleblower()

        while not any(queue_.qsize() != 0 for queue_ in self.queue_pool.queue_lookup.values()):
            time.sleep(0.001)

        time.sleep(5)

        self.listener_snapshot_service.run()

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

        remaining_threads = [
            thread for thread in threading.enumerate()
            if thread is not threading.current_thread() and thread.is_alive()
        ]

        if remaining_threads:
            self.logger.warning(f"Some threads are still alive:")
            for thread in remaining_threads:
                self.logger.warning(f"Thread {thread.name} is still alive {thread.is_alive()}")
        else:
            self.logger.info("All threads have been successfully stopped.")
