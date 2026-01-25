from __future__ import annotations

import pprint
import sys
import threading
import time

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.core.queue_pool import DataSinkQueuePool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.stream_service import StreamService
from binance_data_processor.enums.data_sink_config import DataSinkCfg
from binance_data_processor.enums.data_save_target_enum import DataSaveTarget

__all__ = [
    'launch_data_sink',
    'BinanceDataSink'
]

def launch_data_sink(cfg: DataSinkCfg = None) -> BinanceDataSink:

    binance_data_sink = BinanceDataSink(cfg)
    binance_data_sink.run()

    return binance_data_sink


class BinanceDataSink:

    __slots__ = [
        'cfg',
        '_logger',
        'stop_flag',
        '_queue_pool',
        '_stream_service',
        '_cli',
        '_fastapi',
        '_data_saver',
        '_snapshot_service'
    ]

    def __init__(
            self,
            cfg: DataSinkCfg
    ) -> None:
        self.cfg = cfg
        self._logger = setup_logger(should_dump_logs=True)
        self._logger.info("\n%s", binance_archiver_logo)
        self._logger.info("Configuration:\n%s", pprint.pformat(cfg, indent=1))
        if self.cfg.data_save_target is DataSaveTarget.BACKBLAZE:
            self._logger.info(f'Bucket: {cfg.blob_conn_params.backblaze_bucket_name}')

        self.stop_flag = threading.Event()

        self._queue_pool = DataSinkQueuePool()

        self._stream_service = StreamService(self._queue_pool, self.stop_flag, self.cfg)

    def run(self) -> None:

        self._stream_service.run()

        try:
            self._check_if_there_is_no_huge_library_imported()
        except Exception as e:
            self._logger.info(f'Exception occurred: {e}')
            self.stop()

    @staticmethod
    def _check_if_there_is_no_huge_library_imported():
        for lib in ['pandas', 'numpy', 'alive_progress']:
            if lib in list(sys.modules.keys()):
                raise Exception(f'Imported {lib} whilst running data sink. Check the imports')

    def stop(self):

        self._logger.info("Shutting down archiver")

        self.stop_flag.set()

        self._fastapi.shutdown()

        time.sleep(5)

        remaining_threads = [t for t in threading.enumerate() if t is not threading.current_thread() and t.is_alive()]

        if remaining_threads:
            self._logger.warning(f"Some threads are still alive:")
            for thread in remaining_threads:
                self._logger.warning(f"Thread {thread.name} is still alive {thread.is_alive()}")
        else:
            self._logger.info("All threads have been successfully stopped.")
