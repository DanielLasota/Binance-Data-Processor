import os
import time
from typing import List
import pprint

# from binance_archiver.orderbook_level_2_listener.setup_logger import setup_logger
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.ArchiverDaemon import ArchiverDaemon
from binance_archiver.logo import logo
from binance_archiver.orderbook_level_2_listener.setup_logger import setup_logger


class DaemonManager:

    def __init__(
            self,
            config: dict,
            dump_path: str = 'temp',
            dump_path_to_log_file: str = 'logs/',
            azure_blob_parameters_with_key: str | None = None,
            container_name: str | None = None,
    ) -> None:
        self.config = config
        self.dump_path = dump_path
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.container_name = container_name
        self.daemons = []
        self.logger = setup_logger(dump_path_to_log_file, dump_logs_to_files=False)

    def run(self) -> None:

        self.logger.info('\n%s', logo)
        self.logger.info('Starting Binance Archiver...')
        self.logger.info("Configuration:\n%s", pprint.pformat(self.config, indent=1))

        self.daemons = self._start_daemon(
            self.dump_path,
            save_to_json=self.config['daemons']['save_to_json'],
            save_to_zip=self.config['daemons']['save_to_zip'],
            send_zip_to_blob=self.config['daemons']['send_zip_to_blob']
        )

        # while True:
        #     time.sleep(10)

    def _start_daemon(
            self,
            dump_path,
            save_to_json: bool = False,
            save_to_zip: bool = False,
            send_zip_to_blob: bool = True
    ) -> List[ArchiverDaemon]:

        if dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(dump_path)

        config = self.config
        file_duration_seconds = config['daemons']['file_duration_seconds']
        snapshot_fetcher_interval_seconds = config['daemons']['snapshot_fetcher_interval_seconds']
        websocket_life_time_seconds = config['daemons']['websocket_life_time_seconds']
        websocket_overlap_seconds = config['daemons']['websocket_overlap_seconds']
        daemons = []

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
                file_duration_seconds=file_duration_seconds,
                dump_path=dump_path,
                websockets_lifetime_seconds=websocket_life_time_seconds,
                websocket_overlap_seconds=websocket_overlap_seconds,
                save_to_json=save_to_json,
                save_to_zip=save_to_zip,
                send_zip_to_blob=send_zip_to_blob
            )

            daemons.append(daemon)
            time.sleep((websocket_overlap_seconds+10) * 2)

        return daemons
