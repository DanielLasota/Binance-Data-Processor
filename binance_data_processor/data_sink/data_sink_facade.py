from __future__ import annotations

import pprint
import sys
import threading
import time

from binance_data_processor.core.command_line_interface import CommandLineInterface
from binance_data_processor.enums.data_save_target_enum import DataSaveTarget
from binance_data_processor.enums.data_sink_config import DataSinkConfig
from binance_data_processor.core.fastapi_manager import FastAPIManager
from binance_data_processor.core.stream_data_saver_and_sender import StreamDataSaverAndSender
from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.core.queue_pool import DataSinkQueuePool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.snapshot_manager import DataSinkDepthSnapshotStrategy, DepthSnapshotService
from binance_data_processor.core.stream_service import StreamService

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
        '_logger',
        'global_shutdown_flag',
        '_queue_pool',
        '_stream_service',
        '_command_line_interface',
        '_fast_api_manager',
        '_stream_data_saver_and_sender',
        '_depth_snapshot_service'
    ]

    def __init__(
            self,
            data_sink_config: DataSinkConfig
    ) -> None:
        self.data_sink_config = data_sink_config
        self._logger = setup_logger(should_dump_logs=True)
        self._logger.info("\n%s", binance_archiver_logo)
        self._logger.info("Configuration:\n%s", pprint.pformat(data_sink_config, indent=1))
        if self.data_sink_config.data_save_target is DataSaveTarget.BACKBLAZE:
            self._logger.info(f'Bucket: {data_sink_config.storage_connection_parameters.backblaze_bucket_name}')

        self.global_shutdown_flag = threading.Event()

        self._queue_pool = DataSinkQueuePool()

        self._stream_service = StreamService(
            queue_pool=self._queue_pool,
            global_shutdown_flag=self.global_shutdown_flag,
            data_sink_config=data_sink_config
        )

        self._stream_data_saver_and_sender = StreamDataSaverAndSender(
            queue_pool=self._queue_pool,
            global_shutdown_flag=self.global_shutdown_flag,
            data_sink_config = self.data_sink_config
        )

        self._command_line_interface = CommandLineInterface(
            stream_service=self._stream_service,
            data_sink_config=self.data_sink_config,
            shutdown_callback=self.shutdown
        )

        self._fast_api_manager = FastAPIManager(
            callback=self._command_line_interface.handle_command
        )

        self._depth_snapshot_service = DepthSnapshotService(
            snapshot_strategy=DataSinkDepthSnapshotStrategy(
                data_saver=self._stream_data_saver_and_sender
            ),
            data_sink_config=data_sink_config,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def run(self) -> None:

        self._stream_service.run()

        self._fast_api_manager.run()

        self._stream_data_saver_and_sender.run()

        self._depth_snapshot_service.run()

        self._check_if_there_is_no_huge_library_imported()

    @staticmethod
    def _check_if_there_is_no_huge_library_imported():
        modules = list(sys.modules.keys())

        for library in ['pandas', 'numpy', 'alive_progress']:
            if library in modules:
                raise Exception(f'Imported {library} whilst running data sink. Check the imports')

    def shutdown(self):

        self._logger.info("Shutting down archiver")

        self.global_shutdown_flag.set()

        self._fast_api_manager.shutdown()

        time.sleep(5)

        remaining_threads = [
            thread for thread in threading.enumerate()
            if thread is not threading.current_thread() and thread.is_alive()
        ]

        if remaining_threads:
            self._logger.warning(f"Some threads are still alive:")
            for thread in remaining_threads:
                self._logger.warning(f"Thread {thread.name} is still alive {thread.is_alive()}")
        else:
            self._logger.info("All threads have been successfully stopped.")
