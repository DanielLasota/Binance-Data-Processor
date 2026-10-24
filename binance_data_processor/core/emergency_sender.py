from __future__ import annotations

import os
import time

from binance_data_processor import DataSinkCfg
from binance_data_processor.cloud_storage_clients.s3_client import S3Client
from binance_data_processor.core.setup_logger import setup_logger


class EmergencySender:

    __slots__ = (
        "cfg",
        "stop_flag",
        "_client",
        "logger",
        "retry_interval_seconds",
    )

    def __init__(
            self,
            cfg: DataSinkCfg,
            stop_flag,
            retry_interval_seconds: int = 60*60,
    ):
        self.cfg = cfg
        self.stop_flag = stop_flag
        self._client = S3Client(self.cfg.blob_conn_params)
        self.logger = None
        self.retry_interval_seconds = retry_interval_seconds

    def start(self):
        self.logger = setup_logger(dump=True)
        self._main_loop()

    def stop(self) -> None:
        self.stop_flag.set()

    def _main_loop(self) -> None:
        while not self.stop_flag.is_set():
            self._sleep_with_flag_check(self.retry_interval_seconds)
            if self.stop_flag.is_set():
                break
            self._flush_zip_dir_once()

    def _sleep_with_flag_check(self, duration: int) -> None:
        for _ in range(duration):
            if self.stop_flag.is_set():
                break
            time.sleep(1)

    def _flush_zip_dir_once(self) -> None:
        files = [f for f in os.listdir(self.cfg.save_path) if f.endswith(".zip")]

        for f in files:

            f_path = f"{self.cfg.save_path}/{f}"
            try:
                self._client.upload_existing_file(file_path=f_path)
                os.remove(f_path)
            except Exception as e:
                self.logger.debug(f"FailedZipReserveSender: retry upload failed for {f}: {e}")
                continue
