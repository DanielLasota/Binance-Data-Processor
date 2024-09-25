from __future__ import annotations

import json
import logging
import os
import pprint
import queue
import time
import traceback
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from collections import defaultdict

from azure.storage.blob import BlobServiceClient
import io
import threading
import requests
from queue import Queue

from binance_archiver.logo import logo
from .setup_logger import setup_logger
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from .exceptions import WebSocketLifeTimeException, BadAzureParameters, BadConfigException
from .abstract_base_classes import Subject, Observer
from .fastapi_manager import FastAPIManager
from .stream_listener import StreamListener
from .trade_queue import TradeQueue
from .difference_depth_queue import DifferenceDepthQueue
from .url_factory import URLFactory


class ListenerFacade(Subject):
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
            instruments=self.instruments,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self._observers = init_observers if init_observers is not None else []

        self.whistleblower = Whistleblower(
            logger=self.logger,
            observers=self._observers,
            global_queue=self.queue_pool.global_queue,
            global_shutdown_flag=self.global_shutdown_flag
        )

        snapshot_strategy = ListenerSnapshotStrategy(global_queue=self.queue_pool.global_queue)

        self.snapshot_manager = SnapshotManager(
            instruments=self.instruments,
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

        websockets_lifetime_seconds = self.config["websocket_life_time_seconds"]
        snapshot_fetcher_interval_seconds = self.config["snapshot_fetcher_interval_seconds"]

        self.stream_service.run_streams(websockets_lifetime_seconds=websockets_lifetime_seconds)

        self.whistleblower.run_whistleblower()

        while not any(queue_.qsize() != 0 for queue_ in self.queue_pool.queue_lookup.values()):
            time.sleep(0.001)

        time.sleep(5)

        self.snapshot_manager.run_snapshots(
            dump_path=dump_path,
            interval=snapshot_fetcher_interval_seconds
        )

    def shutdown(self):
        self.logger.info("Shutting down archiver")
        self.global_shutdown_flag.set()

        # time.sleep(10)

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
    def __init__(
            self,
            config: dict,
            logger: logging.Logger,
            azure_blob_parameters_with_key: str | None = None,
            container_name: str | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.container_name = container_name
        self.instruments = config["instruments"]
        self.global_shutdown_flag = threading.Event()

        self.queue_pool = QueuePoolDataSink()
        self.stream_service = StreamService(
            instruments=self.instruments,
            logger=self.logger,
            queue_pool=self.queue_pool,
            global_shutdown_flag=self.global_shutdown_flag
        )

        self.command_line_interface = CommandLineInterface(
            instruments=self.instruments,
            logger=self.logger,
            stream_service=self.stream_service
        )
        self.fast_api_manager = FastAPIManager()
        self.fast_api_manager.set_callback(self.command_line_interface.handle_command)

        self.data_saver = DataSaver(
            logger=self.logger,
            azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
            container_name=self.container_name,
            global_shutdown_flag=self.global_shutdown_flag
        )

        snapshot_strategy = DataSinkSnapshotStrategy(
            data_saver=self.data_saver,
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager = SnapshotManager(
            instruments=self.instruments,
            logger=self.logger,
            snapshot_strategy=snapshot_strategy,
            global_shutdown_flag=self.global_shutdown_flag
        )

    def run(self) -> None:
        dump_path = self.config.get("dump_path", "dump/")
        file_duration_seconds = self.config["file_duration_seconds"]
        websockets_lifetime_seconds = self.config["websocket_life_time_seconds"]
        snapshot_fetcher_interval_seconds = self.config["snapshot_fetcher_interval_seconds"]

        self.stream_service.run_streams(websockets_lifetime_seconds=websockets_lifetime_seconds)

        self.fast_api_manager.run()

        self.data_saver.run_data_saver(
            queue_pool=self.queue_pool,
            dump_path=dump_path,
            file_duration_seconds=file_duration_seconds,
            save_to_json=self.config["save_to_json"],
            save_to_zip=self.config["save_to_zip"],
            send_zip_to_blob=self.config["send_zip_to_blob"]
        )

        self.snapshot_manager.run_snapshots(
            dump_path=dump_path,
            interval=snapshot_fetcher_interval_seconds
        )

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


class Whistleblower:
    def __init__(
            self,
            logger: logging.Logger,
            observers: list[Observer],
            global_queue: Queue,
            global_shutdown_flag: threading.Event
    ) -> None:
        self.logger = logger
        self.observers = observers
        self.global_queue = global_queue
        self.global_shutdown_flag = global_shutdown_flag

    def process_global_queue(self):
        while not self.global_shutdown_flag.is_set():
            if self.global_queue.qsize() > 100:
                self.logger.warning(f'qsize: {self.global_queue.qsize()}')
            try:
                message = self.global_queue.get(timeout=1)
                for observer in self.observers:
                    observer.update(message)
                # self.global_queue.task_done()
            except queue.Empty:
                continue

    def run_whistleblower(self):
        whistleblower_thread = threading.Thread(target=self.process_global_queue)
        whistleblower_thread.daemon = True
        whistleblower_thread.start()


class QueuePoolDataSink:
    def __init__(self):

        self.spot_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.SPOT)
        self.spot_trade_stream_message_queue = TradeQueue(market=Market.SPOT)

        self.usd_m_futures_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.USD_M_FUTURES)
        self.usd_m_futures_trade_stream_message_queue = TradeQueue(market=Market.USD_M_FUTURES)

        self.coin_m_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.COIN_M_FUTURES)
        self.coin_m_trade_stream_message_queue = TradeQueue(market=Market.COIN_M_FUTURES)

        self.queue_lookup = {
            (Market.SPOT, StreamType.DIFFERENCE_DEPTH): self.spot_orderbook_stream_message_queue,
            (Market.SPOT, StreamType.TRADE): self.spot_trade_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.usd_m_futures_orderbook_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.TRADE): self.usd_m_futures_trade_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.coin_m_orderbook_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.TRADE): self.coin_m_trade_stream_message_queue,
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queue_lookup.get((market, stream_type))


class QueuePoolListener:
    def __init__(self):
        self.global_queue = Queue()

        self.spot_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.SPOT, global_queue=self.global_queue)
        self.spot_trade_stream_message_queue = TradeQueue(market=Market.SPOT, global_queue=self.global_queue)
        self.usd_m_futures_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.USD_M_FUTURES, global_queue=self.global_queue)
        self.usd_m_futures_trade_stream_message_queue = TradeQueue(market=Market.USD_M_FUTURES, global_queue=self.global_queue)
        self.coin_m_orderbook_stream_message_queue = DifferenceDepthQueue(market=Market.COIN_M_FUTURES, global_queue=self.global_queue)
        self.coin_m_trade_stream_message_queue = TradeQueue(market=Market.COIN_M_FUTURES, global_queue=self.global_queue)

        self.queue_lookup = {
            (Market.SPOT, StreamType.DIFFERENCE_DEPTH): self.spot_orderbook_stream_message_queue,
            (Market.SPOT, StreamType.TRADE): self.spot_trade_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.usd_m_futures_orderbook_stream_message_queue,
            (Market.USD_M_FUTURES, StreamType.TRADE): self.usd_m_futures_trade_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH): self.coin_m_orderbook_stream_message_queue,
            (Market.COIN_M_FUTURES, StreamType.TRADE): self.coin_m_trade_stream_message_queue,
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queue_lookup.get((market, stream_type))


class StreamService:
    def __init__(
        self,
        instruments: dict,
        logger: logging.Logger,
        queue_pool: QueuePoolDataSink | QueuePoolListener,
        global_shutdown_flag: threading.Event
    ):
        self.instruments = instruments
        self.logger = logger
        self.queue_pool = queue_pool
        self.global_shutdown_flag = global_shutdown_flag

        self.is_someone_overlapping_right_now_flag = threading.Event()
        self.stream_listeners = {}
        self.overlap_lock: threading.Lock = threading.Lock()

    def run_streams(self, websockets_lifetime_seconds: int):
        for market_str, pairs in self.instruments.items():
            market = Market[market_str.upper()]
            for stream_type in [StreamType.DIFFERENCE_DEPTH, StreamType.TRADE]:
                self.start_stream_service(
                    stream_type=stream_type,
                    market=market,
                    websockets_lifetime_seconds=websockets_lifetime_seconds
                )

    def start_stream_service(self,stream_type: StreamType,market: Market,websockets_lifetime_seconds: int) -> None:
        queue = self.queue_pool.get_queue(market, stream_type)
        pairs = self.instruments[market.name.lower()]

        thread = threading.Thread(
            target=self._stream_service,
            args=(
                queue,
                pairs,
                stream_type,
                market,
                websockets_lifetime_seconds
            ),
            name=f'stream_service: market: {market}, stream_type: {stream_type}'
        )
        thread.start()

    def _stream_service(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: list[str],
        stream_type: StreamType,
        market: Market,
        websockets_lifetime_seconds: int
    ) -> None:

        def sleep_with_flag_check(duration: int) -> None:
            interval = 1
            for _ in range(0, duration, interval):
                if self.global_shutdown_flag.is_set():
                    break
                time.sleep(interval)

        while not self.global_shutdown_flag.is_set():
            new_stream_listener = None
            old_stream_listener = None

            try:
                old_stream_listener = StreamListener(
                    logger=self.logger,
                    queue=queue,
                    pairs=pairs,
                    stream_type=stream_type,
                    market=market
                )
                self.stream_listeners[(market, stream_type, 'old')] = old_stream_listener

                if stream_type is StreamType.DIFFERENCE_DEPTH:
                    queue.currently_accepted_stream_id = old_stream_listener.id.id
                elif stream_type is StreamType.TRADE:
                    queue.currently_accepted_stream_id = old_stream_listener.id

                old_stream_listener.start_websocket_app()
                new_stream_listener = None

                while not self.global_shutdown_flag.is_set():
                    sleep_with_flag_check(websockets_lifetime_seconds)

                    while self.is_someone_overlapping_right_now_flag.is_set():
                        time.sleep(1)

                    with self.overlap_lock:
                        self.is_someone_overlapping_right_now_flag.set()
                        self.logger.info(f'Started changing procedure {market} {stream_type}')

                        new_stream_listener = StreamListener(
                            logger=self.logger,
                            queue=queue,
                            pairs=pairs,
                            stream_type=stream_type,
                            market=market
                        )

                        new_stream_listener.start_websocket_app()
                        self.stream_listeners[(market, stream_type, 'new')] = new_stream_listener

                    while not queue.did_websockets_switch_successfully and not self.global_shutdown_flag.is_set():
                        time.sleep(1)

                    with self.overlap_lock:
                        self.is_someone_overlapping_right_now_flag.clear()
                    self.logger.info(f"{market} {stream_type} switched successfully")

                    if not self.global_shutdown_flag.is_set():
                        queue.did_websockets_switch_successfully = False

                        old_stream_listener.websocket_app.close()
                        old_stream_listener.thread.join()

                        old_stream_listener = new_stream_listener
                        old_stream_listener.thread = new_stream_listener.thread

                        self.stream_listeners[(market, stream_type, 'new')] = None
                        self.stream_listeners[(market, stream_type, 'old')] = old_stream_listener

            except Exception as e:
                self.logger.error(f'{e}, something bad happened')
                self.logger.error("Traceback (most recent call last):")
                self.logger.error(traceback.format_exc())

            finally:
                if new_stream_listener is not None:
                    for _ in range(10):
                        if new_stream_listener.websocket_app.sock.connected is False:
                            time.sleep(1)
                        else:
                            new_stream_listener.websocket_app.close()
                            break
                if old_stream_listener is not None:
                    for _ in range(10):
                        if old_stream_listener.websocket_app.sock.connected is False:
                            time.sleep(1)
                        else:
                            old_stream_listener.websocket_app.close()
                            break

                if (new_stream_listener is not None and new_stream_listener.websocket_app.sock
                        and new_stream_listener.websocket_app.sock.connected is False):
                    new_stream_listener = None
                    # new_stream_listener_thread = None

                if (old_stream_listener is not None and old_stream_listener.websocket_app.sock
                        and old_stream_listener.websocket_app.sock.connected is False):
                    old_stream_listener = None

                time.sleep(6)

    def update_subscriptions(self, market: Market, asset_upper: str, action: str):
        for stream_type in [StreamType.DIFFERENCE_DEPTH, StreamType.TRADE]:
            for status in ['old', 'new']:
                stream_listener: StreamListener = self.stream_listeners.get((market, stream_type, status))
                if stream_listener:
                    stream_listener.change_subscription(action=action, pair=asset_upper)


class SnapshotStrategy(ABC):
    @abstractmethod
    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        ...


class DataSinkSnapshotStrategy(SnapshotStrategy):
    def __init__(
        self,
        data_saver: DataSaver,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        self.data_saver = data_saver
        self.save_to_json = save_to_json
        self.save_to_zip = save_to_zip
        self.send_zip_to_blob = send_zip_to_blob

    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        file_path = os.path.join(dump_path, file_name)
        if self.save_to_json:
            self.data_saver.save_to_json(snapshot, file_path)
        if self.save_to_zip:
            self.data_saver.save_to_zip(snapshot, file_name, file_path)
        if self.send_zip_to_blob:
            self.data_saver.send_zipped_json_to_blob(snapshot, file_name)


class ListenerSnapshotStrategy(SnapshotStrategy):
    def __init__(self, global_queue: Queue):
        self.global_queue = global_queue

    def handle_snapshot(
        self,
        snapshot: dict,
        pair: str,
        market: Market,
        dump_path: str,
        file_name: str
    ):
        self.global_queue.put(json.dumps(snapshot))


class SnapshotManager:
    def __init__(
        self,
        instruments: dict,
        logger: logging.Logger,
        snapshot_strategy: SnapshotStrategy,
        global_shutdown_flag: threading.Event
    ):
        self.instruments = instruments
        self.logger = logger
        self.snapshot_strategy = snapshot_strategy
        self.global_shutdown_flag = global_shutdown_flag

    def run_snapshots(
        self,
        dump_path: str,
        interval: int
    ):
        for market_str, pairs in self.instruments.items():
            market = Market[market_str.upper()]
            self.start_snapshot_daemon(
                market=market,
                pairs=pairs,
                dump_path=dump_path,
                interval=interval
            )

    def start_snapshot_daemon(
        self,
        market: Market,
        pairs: list[str],
        dump_path: str,
        interval: int
    ):
        thread = threading.Thread(
            target=self._snapshot_daemon,
            args=(
                pairs,
                market,
                dump_path,
                interval
            ),
            name=f'snapshot_daemon: market: {market}'
        )
        thread.start()

    def _snapshot_daemon(
        self,
        pairs: list[str],
        market: Market,
        dump_path: str,
        fetch_interval: int
    ) -> None:
        while not self.global_shutdown_flag.is_set():
            for pair in pairs:
                try:
                    snapshot, request_timestamp, receive_timestamp = self._get_snapshot(pair, market)

                    if snapshot is None:
                        continue

                    snapshot["_rq"] = request_timestamp
                    snapshot["_rc"] = receive_timestamp

                    file_name = DataSaver.get_file_name(
                        pair=pair,
                        market=market,
                        stream_type=StreamType.DEPTH_SNAPSHOT
                    )

                    self.snapshot_strategy.handle_snapshot(
                        snapshot=snapshot,
                        pair=pair,
                        market=market,
                        dump_path=dump_path,
                        file_name=file_name
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error whilst fetching snapshot: {market} {StreamType.DEPTH_SNAPSHOT}: {e}"
                    )

            self._sleep_with_flag_check(fetch_interval)

        self.logger.info(f"{market}: snapshot daemon has ended")

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _get_snapshot(self, pair: str, market: Market) -> tuple[dict[str, any] | None, int | None, int | None]:
        url = URLFactory.get_snapshot_url(market=market, pair=pair)

        try:
            request_timestamp = TimeUtils.get_utc_timestamp_epoch_milliseconds()
            response = requests.get(url, timeout=5)
            receive_timestamp = TimeUtils.get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()
            data = response.json()

            return data, request_timestamp, receive_timestamp

        except Exception as e:
            self.logger.error(f"Error whilst fetching snapshot: {e}")

            return None, None, None


class CommandLineInterface:
    def __init__(
        self,
        instruments: dict,
        logger: logging.Logger,
        stream_service: StreamService,
    ):
        self.instruments = instruments
        self.logger = logger
        self.stream_service = stream_service

    def handle_command(self, message):
        command = list(message.items())[0][0]
        arguments = list(message.items())[0][1]

        if command == 'modify_subscription':
            self.modify_subscription(
                type_=arguments['type'],
                market=arguments['market'],
                asset=arguments['asset']
            )
        else:
            self.logger.warning('Bad command, try again')

    def modify_subscription(self, type_: str, market: str, asset: str):
        asset_upper = asset.upper()
        market_lower = market.lower()

        if type_ == 'subscribe':
            if asset_upper not in self.instruments[market_lower]:
                self.instruments[market_lower].append(asset_upper)
        elif type_ == 'unsubscribe':
            if asset_upper in self.instruments[market_lower]:
                self.instruments[market_lower].remove(asset_upper)

        self.stream_service.update_subscriptions(Market[market.upper()], asset_upper, type_)


class DataSaver:
    def __init__(
        self,
        logger: logging.Logger,
        azure_blob_parameters_with_key: str | None,
        container_name: str | None,
        global_shutdown_flag: threading.Event
    ):
        self.logger = logger
        self.azure_blob_parameters_with_key = azure_blob_parameters_with_key
        self.container_name = container_name
        self.global_shutdown_flag = global_shutdown_flag

        if azure_blob_parameters_with_key and container_name:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                azure_blob_parameters_with_key
            )
            self.container_name = container_name
        else:
            self.blob_service_client = None

    def run_data_saver(
        self,
        queue_pool: QueuePoolDataSink | QueuePoolListener,
        dump_path: str,
        file_duration_seconds: int,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        for (market, stream_type), queue in queue_pool.queue_lookup.items():
            self.start_stream_writer(
                queue=queue,
                market=market,
                file_duration_seconds=file_duration_seconds,
                dump_path=dump_path,
                stream_type=stream_type,
                save_to_json=save_to_json,
                save_to_zip=save_to_zip,
                send_zip_to_blob=send_zip_to_blob
            )

    def start_stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ) -> None:
        thread = threading.Thread(
            target=self._stream_writer,
            args=(
                queue,
                market,
                file_duration_seconds,
                dump_path,
                stream_type,
                save_to_json,
                save_to_zip,
                send_zip_to_blob
            ),
            name=f'stream_writer: market: {market}, stream_type: {stream_type}'
        )
        thread.start()

    def _stream_writer(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        file_duration_seconds: int,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ):
        while not self.global_shutdown_flag.is_set():
            self._process_stream_data(
                queue,
                market,
                dump_path,
                stream_type,
                save_to_json,
                save_to_zip,
                send_zip_to_blob,
            )
            self._sleep_with_flag_check(file_duration_seconds)

        self._process_stream_data(
            queue,
            market,
            dump_path,
            stream_type,
            save_to_json,
            save_to_zip,
            send_zip_to_blob,
        )

        self.logger.info(f"{market} {stream_type}: ended _stream_writer")

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_stream_data(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        market: Market,
        dump_path: str,
        stream_type: StreamType,
        save_to_json: bool,
        save_to_zip: bool,
        send_zip_to_blob: bool
    ) -> None:
        if not queue.empty():

            stream_data = defaultdict(list)

            while not queue.empty():
                message, timestamp_of_receive = queue.get_nowait()
                message = json.loads(message)

                stream = message["stream"]

                if not stream:
                    self.logger.error("Message missing 'stream' key.")
                    continue

                message["_E"] = timestamp_of_receive
                stream_data[stream].append(message)

            for stream, data in stream_data.items():
                _pair = stream.split("@")[0]
                file_name = self.get_file_name(_pair, market, stream_type)
                file_path = os.path.join(dump_path, file_name)

                if save_to_json:
                    self.save_to_json(data, file_path)

                if save_to_zip:
                    self.save_to_zip(data, file_name, file_path)

                if send_zip_to_blob:
                    self.send_zipped_json_to_blob(data, file_name)

    def save_to_json(self, data, file_path) -> None:
        try:
            with open(file_path, "w") as f:
                json.dump(data, f)
        except IOError as e:
            self.logger.error(f"IO error when writing to file {file_path}: {e}")

    def save_to_zip(self, data, file_name, file_path):
        zip_file_path = f"{file_path}.zip"
        try:
            with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_data = json.dumps(data)
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_data)
        except IOError as e:
            self.logger.error(f"IO error when writing to zip file {zip_file_path}: {e}")

    def send_zipped_json_to_blob(self, data, file_name):
        if not self.blob_service_client:
            self.logger.error("Blob service client is not configured.")
            return

        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                json_data = json.dumps(data)
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_data)

            zip_buffer.seek(0)

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=f"{file_name}.zip"
            )

            blob_client.upload_blob(zip_buffer, overwrite=True)
        except Exception as e:
            self.logger.error(f"Error uploading zip to blob: {e}")

    @staticmethod
    def get_file_name(pair: str, market: Market, stream_type: StreamType) -> str:
        pair_lower = pair.lower()
        formatted_now_timestamp = TimeUtils.get_utc_formatted_timestamp_for_file_name()

        market_mapping = {
            Market.SPOT: "spot",
            Market.USD_M_FUTURES: "futures_usd_m",
            Market.COIN_M_FUTURES: "futures_coin_m",
        }

        data_type_mapping = {
            StreamType.DIFFERENCE_DEPTH: "binance_difference_depth",
            StreamType.DEPTH_SNAPSHOT: "binance_snapshot",
            StreamType.TRADE: "binance_trade",
        }

        market_short_name = market_mapping.get(market, "unknown_market")
        prefix = data_type_mapping.get(stream_type, "unknown_data_type")

        return f"{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.json"


class TimeUtils:
    @staticmethod
    def get_utc_formatted_timestamp_for_file_name() -> str:
        return datetime.utcnow().strftime("%d-%m-%YT%H-%M-%SZ")

    @staticmethod
    def get_utc_timestamp_epoch_milliseconds() -> int:
        return round(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def get_utc_timestamp_epoch_seconds() -> int:
        return round(datetime.now(timezone.utc).timestamp())


def launch_data_sink(
        config,
        azure_blob_parameters_with_key: str | None = None,
        container_name: str | None = None,
        should_dump_logs: bool = False
) -> DataSinkFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}

    instruments = config.get("instruments")

    if not instruments or not isinstance(instruments, dict):
        raise BadConfigException("Instruments config is missing or not a dictionary.")

    if not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets.")

    for market, pairs in instruments.items():
        if market not in valid_markets:
            raise BadConfigException(f"Invalid or not handled market: {market}")
        if not pairs or not isinstance(pairs, list):
            raise BadConfigException(f"Pairs for market {market} are missing or invalid.")

    if config['send_zip_to_blob'] and (not azure_blob_parameters_with_key or not container_name):
        raise BadAzureParameters('Azure blob parameters with key or container name is missing or empty')

    if not (60 <= config["websocket_life_time_seconds"] <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Bad websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)

    logger.info("\n%s", logo)
    logger.info("Starting Binance Archiver...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    dump_path = config.get("dump_path", "dump/")
    if dump_path[0] == "/":
        logger.warning(
            "Specified dump_path starts with '/': presumably dump_path is wrong"
        )

    if not os.path.exists(dump_path):
        os.makedirs(dump_path)

    archiver_facade = DataSinkFacade(
        config=config,
        logger=logger,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        container_name=container_name
    )

    archiver_facade.run()

    return archiver_facade

def launch_data_listener(
        config,
        should_dump_logs: bool = False,
        init_observers: list[object] = None
) -> ListenerFacade:

    valid_markets = {"spot", "usd_m_futures", "coin_m_futures"}

    instruments = config.get("instruments")

    if not instruments or not isinstance(instruments, dict):
        raise BadConfigException("Instruments config is missing or not a dictionary.")

    if not (0 < len(instruments) <= 3):
        raise BadConfigException("Config must contain 1 to 3 markets.")

    for market, pairs in instruments.items():
        if market not in valid_markets:
            raise BadConfigException(f"Invalid or not handled market: {market}")
        if not pairs or not isinstance(pairs, list):
            raise BadConfigException(f"Pairs for market {market} are missing or invalid.")

    if not (30 <= config["websocket_life_time_seconds"] <= 60 * 60 * 23):
        raise WebSocketLifeTimeException('Bad websocket_life_time_seconds')

    logger = setup_logger(should_dump_logs=should_dump_logs)

    logger.info("\n%s", logo)
    logger.info("Starting Binance Listener...")
    logger.info("Configuration:\n%s", pprint.pformat(config, indent=1))

    listener_facade = ListenerFacade(
        config=config,
        logger=logger,
        init_observers=init_observers
    )

    listener_facade.run()

    return listener_facade
