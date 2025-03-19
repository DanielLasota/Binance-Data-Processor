from __future__ import annotations

import pprint
import threading
import time

from binance_data_processor.command_line_interface import CommandLineInterface
from binance_data_processor.data_sink_config import DataSinkConfig
from binance_data_processor.fastapi_manager import FastAPIManager
from binance_data_processor.stream_data_saver_and_sender import StreamDataSaverAndSender
from binance_data_processor.logo import binance_archiver_logo
from binance_data_processor.queue_pool import DataSinkQueuePool
from binance_data_processor.setup_logger import setup_logger
from binance_data_processor.snapshot_manager import DataSinkDepthSnapshotStrategy, DepthSnapshotService
from binance_data_processor.stream_service import StreamService

__all__ = [
    'launch_data_sink',
    'BinanceDataSink'
]

def launch_data_sink(data_sink_config: DataSinkConfig = None) -> BinanceDataSink:

    if data_sink_config is None:
        data_sink_config = DataSinkConfig()

    binance_data_sink = BinanceDataSink(data_sink_config=data_sink_config)
    binance_data_sink.run()

    return binance_data_sink


class BinanceDataSink:

    __slots__ = [
        'data_sink_config',
        'logger',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        'command_line_interface',
        'fast_api_manager',
        'stream_data_saver_and_sender',
        'depth_snapshot_service'
    ]

    def __init__(
            self,
            data_sink_config: DataSinkConfig
    ) -> None:
        self.data_sink_config = data_sink_config
        self.logger = setup_logger(should_dump_logs=True)
        self.logger.info("\n%s", binance_archiver_logo)
        self.logger.info("Configuration:\n%s", pprint.pformat(data_sink_config, indent=1))

        self.global_shutdown_flag = threading.Event()

        self.queue_pool = DataSinkQueuePool()

        self.stream_service = StreamService(
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag,
            data_sink_config=data_sink_config
        )

        self.stream_data_saver_and_sender = StreamDataSaverAndSender(
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag,
            data_sink_config = self.data_sink_config
        )

        self.command_line_interface = CommandLineInterface(
            stream_service=self.stream_service,
            data_sink_config=self.data_sink_config,
            shutdown_callback=self.shutdown
        )

        self.fast_api_manager = FastAPIManager(callback=self.command_line_interface.handle_command)

        self.depth_snapshot_service = DepthSnapshotService(
            snapshot_strategy=DataSinkDepthSnapshotStrategy(data_saver=self.stream_data_saver_and_sender),
            data_sink_config=data_sink_config,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def run(self) -> None:

        self.stream_service.run()

        self.fast_api_manager.run()

        self.stream_data_saver_and_sender.run()

        self.depth_snapshot_service.run()

    def shutdown(self):

        self.logger.info("Shutting down archiver")

        self.global_shutdown_flag.set()

        self.fast_api_manager.shutdown()

        time.sleep(5)

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
