from __future__ import annotations

import pprint
import time
import threading

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.core.listener_observer_updater import Whistleblower
from binance_data_processor.core.queue_pool import ListenerQueuePool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.abstract_base_classes import Subject, Observer
from binance_data_processor.core.snapshot_manager import ListenerDepthSnapshotStrategy, SnapshotService
from binance_data_processor.core.stream_service import StreamService
from binance_data_processor.enums.data_sink_config import DataSinkCfg

__all__ = [
    'launch_data_listener',
    'BinanceDataListener'
]


def launch_data_listener(
        cfg: DataSinkCfg = DataSinkCfg(),
        observers: list[object] = None
) -> BinanceDataListener:

    listener = BinanceDataListener(cfg, observers)

    listener.run()
    return listener

class BinanceDataListener(Subject):

    __slots__ = [
        'cfg',
        'logger',
        'global_shutdown_flag',
        '_queue_pool',
        '_stream_service',
        '_observers',
        '_whistleblower',
        '_snapshot_service'
    ]

    def __init__(
            self,
            cfg: DataSinkCfg,
            init_observers: list[Observer] | None = None
    ) -> None:

        self.cfg = cfg

        self.logger = setup_logger(should_dump_logs=True)
        if cfg.show_logo is True:
            self.logger.info("\n%s", binance_archiver_logo)
            self.logger.info("Configuration:\n%s", pprint.pformat(cfg, indent=1))

        self.global_shutdown_flag = threading.Event()

        self._queue_pool = ListenerQueuePool()

        self._observers = init_observers if init_observers is not None else []

        self._stream_service = StreamService(
            queue_pool=self._queue_pool,
            stop_flag=self.global_shutdown_flag,
            cfg=cfg
        )

        self._whistleblower = Whistleblower(
            observers=self._observers,
            global_queue=self._queue_pool.global_queue,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self._snapshot_service = SnapshotService(
            snapshot_strategy=ListenerDepthSnapshotStrategy(global_queue=self._queue_pool.global_queue),
            cfg=cfg,
            stop_flag=self.global_shutdown_flag
        )

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self, message) -> None:
        for observer in self._observers:
            observer.update(message)

    def run(self) -> None:

        self._stream_service.run()

        self._whistleblower.run()

        while not any(queue_.qsize() != 0 for queue_ in self._queue_pool.queue_lookup.values()):
            time.sleep(0.001)

        time.sleep(5)

        self._snapshot_service.run()

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

        remaining_threads = [t for t in threading.enumerate() if t is not threading.current_thread() and t.is_alive()]

        if remaining_threads:
            self.logger.warning(f"Some threads are still alive:")
            for thread in remaining_threads:
                self.logger.warning(f"Thread {thread.name} is still alive {thread.is_alive()}")
        else:
            self.logger.info("All threads have been successfully stopped.")
