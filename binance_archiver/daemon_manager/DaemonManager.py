import os
import time
from typing import Optional
import threading
from binance_archiver.orderbook_level_2_listener.setup_logger import setup_logger
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.ArchiverDaemon import ArchiverDaemon
from binance_archiver.logo import logo
from datetime import datetime, timezone


class DaemonManager:

    def __init__(
            self,
            config: dict,
            dump_path: str = 'temp',
            remove_csv_after_zip: bool = True,
            remove_zip_after_upload: bool = True,
            send_zip_to_blob: bool = True,
            dump_path_to_log_file: str = 'logs/',
            azure_blob_parameters_with_key: str | None = None,
            container_name: str | None = None
    ) -> None:
        self.config = config
        self.logger = setup_logger(dump_path_to_log_file)
        self.dump_path = dump_path
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.container_name = container_name
        self.daemons = []
        self.remove_csv_after_zip = remove_csv_after_zip
        self.remove_zip_after_upload = remove_zip_after_upload
        self.send_zip_to_blob = send_zip_to_blob

        self.logger.info(logo)
        self.logger.info('Starting Binance Archiver...')

    def start_daemon(self):
        if self.azure_blob_parameters_with_key is None or self.container_name is None:
            self.send_zip_to_blob = False

        if self.dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(self.dump_path)

        config = self.config
        listen_duration = config['daemons']['listen_duration']
        snapshot_fetcher_interval_seconds = config['daemons']['snapshot_fetcher_interval_seconds']

        new_daemons = []

        for market_type, instruments in config['daemons']['markets'].items():
            market_enum = Market[market_type.upper()]
            daemon = ArchiverDaemon(
                logger=self.logger,
                azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
                container_name=self.container_name,
                remove_csv_after_zip=self.remove_csv_after_zip,
                remove_zip_after_upload=self.remove_zip_after_upload,
                send_zip_to_blob=self.send_zip_to_blob,
                snapshot_fetcher_interval_seconds=snapshot_fetcher_interval_seconds
            )

            daemon.run(
                pairs=instruments,
                market=market_enum,
                file_duration_seconds=listen_duration,
                dump_path=self.dump_path
            )
            new_daemons.append(daemon)

        return new_daemons

    def run(self):
        daemon_service_life_time_seconds = self.config['daemons']['daemon_service_life_time_seconds']
        overlap_time = 20

        self.daemons = self.start_daemon()

        while True:
            time.sleep(daemon_service_life_time_seconds - overlap_time)

            self.logger.info(">>>>>> Starting new daemon 1")
            new_daemons = self.start_daemon()

            # self.logger.info(f"New daemons will fully take over in {overlap_time} seconds.")

            time.sleep(overlap_time)

            for daemon in self.daemons:
                self.logger.info(">>>>>> Stopping old daemons 2")
                daemon.global_shutdown_flag.set()

            self.daemons = new_daemons
