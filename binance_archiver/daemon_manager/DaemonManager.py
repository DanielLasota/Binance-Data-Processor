import os
import time
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
            dump_path_to_log_file: str = 'logs/',
            azure_blob_parameters_with_key: str | None = None,
            container_name: str | None = None,
            save_to_json: bool = True,
            save_to_zip: bool = True,
            send_zip_to_blob: bool = True
    ) -> None:
        self.config = config
        self.logger = setup_logger(dump_path_to_log_file)
        self.dump_path = dump_path
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.container_name = container_name
        self.daemons = []

        self.logger.info(logo)
        self.logger.info('Starting Binance Archiver...')

    def start_daemon(self):

        if self.dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(self.dump_path)

        config = self.config
        listen_duration = config['daemons']['listen_duration']
        snapshot_fetcher_interval_seconds = config['daemons']['snapshot_fetcher_interval_seconds']
        websocket_life_time_seconds = config['daemons']['websocket_life_time_seconds']
        websocket_overlap_seconds = config['daemons']['websocket_overlap_seconds']
        new_daemons = []

        for market_type, instruments in config['daemons']['markets'].items():
            market_enum = Market[market_type.upper()]
            daemon = ArchiverDaemon(
                logger=self.logger,
                azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
                container_name=self.container_name,
                snapshot_fetcher_interval_seconds=snapshot_fetcher_interval_seconds
            )

            daemon.run(
                pairs=instruments,
                market=market_enum,
                file_duration_seconds=listen_duration,
                dump_path=self.dump_path,
                websockets_lifetime=websocket_life_time_seconds,
                overlap=websocket_overlap_seconds,
                save_to_json=False,
                save_to_zip=False,
                send_zip_to_blob=True,
            )
            new_daemons.append(daemon)

        return new_daemons

    def run(self):
        websocket_life_time_seconds = self.config['daemons']['websocket_life_time_seconds']
        overlap_time = 20

        self.daemons = self.start_daemon()

        while True:
            time.sleep(10)
            # time.sleep(daemon_service_life_time_seconds - overlap_time)
            #
            # self.logger.info(">>>>>> Starting new daemon 1")
            # new_daemons = self.start_daemon()
            #
            # # self.logger.info(f"New daemons will fully take over in {overlap_time} seconds.")
            #
            # time.sleep(overlap_time)
            #
            # for daemon in self.daemons:
            #     self.logger.info(">>>>>> Stopping old daemons 2")
            #     daemon.global_shutdown_flag.set()
            #
            # self.daemons = new_daemons
