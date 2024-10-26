from __future__ import annotations

import logging
import os
import pprint
import time
import threading

from binance_archiver.logo import binance_archiver_logo
from .commandline_interface import CommandLineInterface
from .data_saver_sender import DataSaverSender
from .listener_observer_updater import ListenerObserverUpdater
from .queue_pool import QueuePoolListener, QueuePoolDataSink
from .setup_logger import setup_logger
from .exceptions import WebSocketLifeTimeException, BadStorageProviderParameters, BadConfigException
from .abstract_base_classes import Subject, Observer
from .fastapi_manager import FastAPIManager
from .snapshot_manager import DataSinkSnapshotStrategy, ListenerSnapshotStrategy, SnapshotManager
from .stream_service import StreamService

__all__ = [
    'launch_data_listener',
    'launch_data_sink'
]


def launch_data_sink(
        config,
        azure_blob_parameters_with_key: str | None = None,
        azure_container_name: str | None = None,
        backblaze_access_key_id: str | None = None,
        backblaze_secret_access_key: str | None = None,
        backblaze_endpoint_url: str | None = None,
        backblaze_bucket_name: str | None = None,
        should_dump_logs: bool = False
) -> DataSinkFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}
    instruments = config.get("instruments")

    if not isinstance(instruments, dict) or not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets and must be a dictionary.")

    for market, pairs in instruments.items():
        if market not in valid_markets or not isinstance(pairs, list):
            raise BadConfigException(f"Invalid pairs for market {market} or market not handled.")
        if not pairs:
            raise BadConfigException(f"Pairs for market {market} are missing or invalid.")

    send_zip_to_blob = config.get('send_zip_to_blob', False)
    if send_zip_to_blob:
        azure_params_ok = azure_blob_parameters_with_key is not None and azure_container_name is not None
        backblaze_params_ok = (backblaze_access_key_id is not None and
                               backblaze_secret_access_key is not None and
                               backblaze_endpoint_url is not None and
                               backblaze_bucket_name is not None)

        if not azure_params_ok and not backblaze_params_ok:
            raise BadStorageProviderParameters(
                'At least one of the Azure or Backblaze parameter sets must be fully specified.')

    if not (60 <= config.get("websocket_life_time_seconds", 0) <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Invalid websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)
    logger.info("\n%s", binance_archiver_logo)
    logger.info("Starting Binance Archiver...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    dump_path = config.get("dump_path", "dump/")
    if dump_path.startswith("/"):
        logger.warning("Specified dump_path starts with '/': presumably dump_path is wrong.")

    os.makedirs(dump_path, exist_ok=True)

    backblaze_s3_parameters = None
    if all([backblaze_access_key_id, backblaze_secret_access_key, backblaze_endpoint_url]):
        backblaze_s3_parameters = {
            'access_key_id': backblaze_access_key_id,
            'secret_access_key': backblaze_secret_access_key,
            'endpoint_url': backblaze_endpoint_url
        }

    archiver_facade = DataSinkFacade(
        config=config,
        logger=logger,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        azure_container_name=azure_container_name,
        backblaze_s3_parameters=backblaze_s3_parameters,
        backblaze_bucket_name=backblaze_bucket_name
    )

    archiver_facade_thread = threading.Thread(target=archiver_facade.run)
    archiver_facade_thread.start()

    return archiver_facade

def launch_data_listener(
        config,
        should_dump_logs: bool = False,
        init_observers: list[object] = None
) -> ListenerFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}
    instruments = config.get("instruments")

    if not isinstance(instruments, dict) or not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets and must be a dictionary.")

    for market, pairs in instruments.items():
        if market not in valid_markets or not isinstance(pairs, list):
            raise BadConfigException(f"Invalid pairs for market {market}.")

    websocket_lifetime = config.get("websocket_life_time_seconds", 0)
    if not (30 <= websocket_lifetime <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Bad websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)
    logger.info("\n%s", binance_archiver_logo)
    logger.info("Starting Binance Listener...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    listener_facade = ListenerFacade(
        config=config,
        logger=logger,
        init_observers=init_observers
    )

    listener_facade.run()
    return listener_facade


class ListenerFacade(Subject):

    __slots__ = [
        'config',
        'logger',
        'instruments',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        '_observers',
        'whistleblower',
        'snapshot_manager'
    ]

    def __init__(
            self,
            config: dict,
            logger: logging.Logger,
            init_observers: list[Observer] | None = None
    ) -> None:
        self.config = config
        self.logger = logger
        self.instruments = config["instruments"]
        self.global_shutdown_flag = threading.Event()

        self.queue_pool = QueuePoolListener()
        self.stream_service = StreamService(
            config=config,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self._observers = init_observers if init_observers is not None else []

        self.whistleblower = ListenerObserverUpdater(
            logger=self.logger,
            observers=self._observers,
            global_queue=self.queue_pool.global_queue,
            global_shutdown_flag=self.global_shutdown_flag
        )

        snapshot_strategy = ListenerSnapshotStrategy(global_queue=self.queue_pool.global_queue)

        self.snapshot_manager = SnapshotManager(
            config=self.config,
            logger=self.logger,
            snapshot_strategy=snapshot_strategy,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self, message) -> None:
        for observer in self._observers:
            observer.update(message)

    def run(self) -> None:
        dump_path = self.config.get("dump_path", "dump/")

        snapshot_fetcher_interval_seconds = self.config["snapshot_fetcher_interval_seconds"]

        self.stream_service.run_streams()

        self.whistleblower.run_whistleblower()

        while not any(queue_.qsize() != 0 for queue_ in self.queue_pool.queue_lookup.values()):
            time.sleep(0.001)

        time.sleep(5)

        self.snapshot_manager.run_snapshots(
            dump_path=dump_path
        )

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

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


class DataSinkFacade:

    __slots__ = [
        'config',
        'logger',
        'azure_blob_parameters_with_key',
        'azure_container_name',
        'backblaze_s3_parameters',
        'backblaze_bucket_name',
        'instruments',
        'global_shutdown_flag',
        'queue_pool',
        'stream_service',
        'command_line_interface',
        'fast_api_manager',
        'data_saver',
        'snapshot_manager'
    ]

    def __init__(
            self,
            config: dict,
            logger: logging.Logger,
            azure_blob_parameters_with_key: str | None = None,
            azure_container_name: str | None = None,
            backblaze_s3_parameters: dict[str, str] | None = None,
            backblaze_bucket_name: str | None = None
    ) -> None:
        self.config = config
        self.logger = logger
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.azure_container_name = azure_container_name
        self.backblaze_s3_parameters = backblaze_s3_parameters
        self.backblaze_bucket_name = backblaze_bucket_name
        self.instruments = config["instruments"]
        self.global_shutdown_flag = threading.Event()

        self.queue_pool = QueuePoolDataSink()
        self.stream_service = StreamService(
            config=self.config,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self.command_line_interface = CommandLineInterface(
            config=self.config,
            logger=self.logger,
            stream_service=self.stream_service
        )
        self.fast_api_manager = FastAPIManager()
        self.fast_api_manager.set_callback(self.command_line_interface.handle_command)

        self.data_saver = DataSaverSender(
            logger=self.logger,
            config=self.config,
            azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
            azure_container_name=self.azure_container_name,
            backblaze_s3_parameters=self.backblaze_s3_parameters,
            backblaze_bucket_name=self.backblaze_bucket_name,
            global_shutdown_flag=self.global_shutdown_flag
        )

        snapshot_strategy = DataSinkSnapshotStrategy(
            data_saver=self.data_saver,
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager = SnapshotManager(
            config=self.config,
            logger=self.logger,
            snapshot_strategy=snapshot_strategy,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def run(self) -> None:
        dump_path = self.config.get("dump_path", "dump/")

        self.stream_service.run_streams()

        self.fast_api_manager.run()

        self.data_saver.run_data_saver(
            queue_pool=self.queue_pool,
            dump_path=dump_path,
            file_duration_seconds=self.config["file_duration_seconds"],
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager.run_snapshots(dump_path=dump_path)

        while not self.global_shutdown_flag.is_set():
            time.sleep(4)

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

        self.fast_api_manager.shutdown()

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
