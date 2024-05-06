import json
import os
import time
from dotenv import load_dotenv
import threading
from orderbook_level_2_listener.setup_logger import setup_logger
from orderbook_level_2_listener.market_enum import Market
from orderbook_level_2_listener.orderbook_level_2_listener import ArchiverDaemon


class DaemonManager:
    """
    Initializes the DaemonManager which manages multiple ArchiverDaemon instances for processing market data.

    :param config_path: Path to the configuration JSON file that contains settings for various daemons.
    :param env_path: Path to the .env file for environment variables needed for configurations such as
                     Azure Blob Storage credentials.
    :param dump_path: Base directory path where all files will be dumped. If not provided, uses the current directory.
    :param should_csv_be_removed_after_zip: Flag to determine whether the CSV files should be deleted after zipping.
    :param should_zip_be_removed_after_upload: Flag to determine whether the zip files should be deleted after being uploaded.
    :param should_zip_be_sent: Flag to determine whether the zip files should be uploaded to Azure Blob Storage.

    Sets up the environment, loads configurations, and prepares to launch data processing daemons as configured.
    """
    def __init__(
            self,
            config_path: str = 'config.json',
            env_path: str = '.env',
            dump_path: str = '',
            should_csv_be_removed_after_zip: bool = True,
            should_zip_be_removed_after_upload: bool = True,
            should_zip_be_sent: bool = True,
            dump_path_to_log_file: str = ''
    ) -> None:
        self.logger = setup_logger(dump_path_to_log_file)
        self.config_path = config_path
        self.env_path = env_path
        self.dump_path = dump_path
        self.daemons = []
        self.should_csv_be_removed_after_zip = should_csv_be_removed_after_zip
        self.should_zip_be_removed_after_upload = should_zip_be_removed_after_upload
        self.should_zip_be_sent = should_zip_be_sent
        self.shutdown_flag = threading.Event()

    def load_config(self):
        """
        Loads the daemon configuration from the specified JSON configuration file.

        :return: A dictionary containing the loaded configuration data.
        """
        with open(self.config_path, 'r') as file:
            return json.load(file)

    def start_daemons(self):
        """
        Initializes and starts all ArchiverDaemon instances as configured in the configuration file.

        This method also ensures that the necessary directories are created as per the dump path,
        loads environment variables, and starts each daemon based on the market and instrument specifications.
        """

        self.logger.info('starting')

        if self.dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(self.dump_path)

        load_dotenv(self.env_path)
        config = self.load_config()
        listen_duration = config['daemons']['listen_duration']

        for market_type, instruments in config['daemons']['markets'].items():
            market_enum = Market[market_type.upper()]
            for instrument in instruments:
                daemon = ArchiverDaemon(
                    logger=self.logger,
                    azure_blob_parameters_with_key=os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY'),
                    container_name=os.environ.get('CONTAINER_NAME'),
                    should_csv_be_removed_after_zip=self.should_csv_be_removed_after_zip,
                    should_zip_be_removed_after_upload=self.should_zip_be_removed_after_upload,
                    should_zip_be_sent=self.should_zip_be_sent,
                    shutdown_flag=self.shutdown_flag  # Pass the shutdown flag to each daemon
                )
                daemon.run(instrument=instrument, market=market_enum, file_duration_seconds=listen_duration,
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
