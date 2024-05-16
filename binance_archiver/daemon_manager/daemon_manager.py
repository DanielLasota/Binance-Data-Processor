import os
import time
from typing import Optional

import threading
from binance_archiver.orderbook_level_2_listener.setup_logger import setup_logger
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.orderbook_level_2_listener import ArchiverDaemon
from binance_archiver.logo import logo


class DaemonManager:
    """
    Initializes the DaemonManager which manages multiple ArchiverDaemon instances for processing market data.

    :param config_path: Path to the configuration JSON file that contains settings for various daemons.
    :param env: Path to the .env file for environment variables needed for configurations such as
                     Azure Blob Storage credentials.
    :param dump_path: Base directory path where all files will be dumped. If not provided, uses the current directory.
    :param remove_csv_after_zip: Flag to determine whether the CSV files should be deleted after zipping.
    :param remove_zip_after_upload: Flag to determine whether the zip files should be deleted after being uploaded.
    :param send_zip_to_blob: Flag to determine whether the zip files should be uploaded to Azure Blob Storage.

    Sets up the environment, loads configurations, and prepares to launch data processing daemons as configured.
    """

    def __init__(
            self,
            config: dict,
            dump_path: str = 'temp',
            remove_csv_after_zip: bool = True,
            remove_zip_after_upload: bool = True,
            send_zip_to_blob: bool = True,
            dump_path_to_log_file: str = 'logs/',
            azure_blob_parameters_with_key: Optional[str] = None,
            container_name: Optional[str] = None
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
        self.shutdown_flag = threading.Event()

    def load_config(self):
        """
        Loads the daemon configuration from the specified JSON

        :return: A dictionary containing the loaded configuration data.
        """
        return self.config

    def start_daemons(self):
        """
        Initializes and starts all ArchiverDaemon instances as configured in the configuration file.

        This method also ensures that the necessary directories are created as per the dump path,
        loads environment variables, and starts each daemon based on the market and instrument specifications.
        """

        if self.azure_blob_parameters_with_key is None or self.container_name is None:
            self.send_zip_to_blob = False

        self.logger.info(logo)
        self.logger.info('Starting Binance Archiver...')

        if self.dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(self.dump_path)

        config = self.load_config()
        listen_duration = config['daemons']['listen_duration']

        for market_type, instruments in config['daemons']['markets'].items():
            market_enum = Market[market_type.upper()]
            for instrument in instruments:
                daemon = ArchiverDaemon(
                    logger=self.logger,
                    azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
                    container_name=self.container_name,
                    remove_csv_after_zip=self.remove_csv_after_zip,
                    remove_zip_after_upload=self.remove_zip_after_upload,
                    send_zip_to_blob=self.send_zip_to_blob,
                    shutdown_flag=self.shutdown_flag
                )
                daemon.run(
                    instrument=instrument,
                    market=market_enum,
                    file_duration_seconds=listen_duration,
                    dump_path=self.dump_path)
                self.daemons.append(daemon)

    def stop_daemons(self):
        """
        Stops all running ArchiverDaemon instances.

        This method is intended to be called when shutting down the manager, ensuring all resources are properly
        released and operations are cleanly terminated.
        """
        self.logger.info("Stopping all daemons")
        self.shutdown_flag.set()
        for daemon in self.daemons:
            daemon.wait()

    def kill(self):
        """
        Forcefully terminates all threads within each daemon.
        """
        self.stop_daemons()  # Gracefully attempts to stop all daemons first
        for daemon in self.daemons:
            daemon.terminate()

    def run(self):
        """
        Runs the DaemonManager, starting all configured daemons and handling KeyboardInterrupt to shut down gracefully.

        This method is the main entry point for running the DaemonManager. It keeps the manager active and checks
        for interruptions to initiate a graceful shutdown.
        """
        self.start_daemons()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_daemons()
