import json
import logging
import os
import threading
import time
import zipfile
from datetime import datetime, timezone
from typing import List, Any, Dict, Tuple
from collections import defaultdict
from azure.storage.blob import BlobServiceClient

from .market_enum import Market
import requests
from queue import Queue
from websocket import WebSocket, WebSocketConnectionClosedException, WebSocketAddressException, \
    WebSocketTimeoutException, WebSocketApp, ABNF
from concurrent.futures import ThreadPoolExecutor

from .NoSignalException import NoSignalException
from .stream_type_enum import StreamType
from .supervisor import Supervisor
from .url_factory import URLFactory


class ArchiverDaemon:
    def __init__(
            self,
            logger: logging.Logger,
            azure_blob_parameters_with_key: str,
            container_name: str,
            remove_csv_after_zip: bool = True,
            remove_zip_after_upload: bool = True,
            send_zip_to_blob: bool = False,
            snapshot_fetcher_interval_seconds: int = 20
    ) -> None:
        self.snapshot_fetcher_interval_seconds = snapshot_fetcher_interval_seconds
        self.logger = logger
        self.remove_csv_after_zip = remove_csv_after_zip
        self.remove_zip_after_upload = remove_zip_after_upload
        self.send_zip_to_blob = send_zip_to_blob
        self.global_shutdown_flag = threading.Event()
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.orderbook_stream_message_queue = Queue()
        self.transaction_stream_message_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=6)

    def run(self, pairs: List[str], market: Market, file_duration_seconds: int, dump_path: str | None = None) -> None:
        self.logger.info(f'launching new daemon: '
                         f'{market} {pairs} {file_duration_seconds}s path: {dump_path}')

        # self.start_orderbook_stream_listener(pairs, market)
        # self.start_orderbook_stream_writer(market, file_duration_seconds, dump_path)
        self.start_transaction_stream_listener(pairs, market)
        self.start_transaction_stream_writer(market, file_duration_seconds, dump_path)
        # self.start_snapshot_daemon(pairs, market, dump_path, self.snapshot_fetcher_interval_seconds)

    def start_orderbook_stream_listener(self, pairs, market) -> None:
        self.executor.submit(self._stream_listener, StreamType.ORDERBOOK, pairs, market)

    def start_transaction_stream_listener(self, pairs, market: Market) -> None:
        self.executor.submit(self._stream_listener, StreamType.TRANSACTIONS, pairs, market)

    def start_orderbook_stream_writer(self, market, file_duration_seconds, dump_path) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.ORDERBOOK)

    def start_transaction_stream_writer(self, market, file_duration_seconds, dump_path) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.TRANSACTIONS)

    def start_snapshot_daemon(self, pairs, market, dump_path, interval):
        self.executor.submit(self._snapshot_daemon, pairs, market, dump_path, interval)

    def _stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market) -> None:
        stream_url_methods = {
            StreamType.ORDERBOOK: URLFactory.get_orderbook_stream_url,
            StreamType.TRANSACTIONS: URLFactory.get_transaction_stream_url
        }

        queues = {
            StreamType.TRANSACTIONS: self.transaction_stream_message_queue,
            StreamType.ORDERBOOK: self.orderbook_stream_message_queue
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)
        queue = queues.get(stream_type, None)

        supervisor_signal_shutdown_flag = threading.Event()
        on_error_shutdown_flag = threading.Event()

        supervisor = Supervisor(
            logger=self.logger,
            stream_type=stream_type,
            market=market,
            on_error_callback=lambda: supervisor_signal_shutdown_flag.set()
        )

        def _on_message(ws, message):
            _timestamp = self._get_utc_timestamp_epoch_milliseconds()
            # self.logger.info(f"Message received: {message}")
            with self.lock:
                queue.put((message, _timestamp))
                supervisor.notify()

        def _on_error(ws, error):
            on_error_shutdown_flag.set()

        def _on_close(ws, close_status_code, close_msg):
            self.logger.info(f"WebSocket connection closed: for {market}, {stream_type}, {pairs} "
                             f"{close_msg} (code: {close_status_code})")
            supervisor.shutdown_supervisor()

        def _on_ping(ws, message):
            # self.logger.info(f"Ping received: {message}")
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            self.logger.info(f"WebSocket connection opened for {market}, {stream_type}, {pairs} ")

        websocket = None

        try:
            websocket = WebSocketApp(
                url,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
                on_ping=_on_ping,
                on_open=_on_open
            )

            websocket_thread = threading.Thread(target=websocket.run_forever)
            websocket_thread.start()

            while True:
                if self.global_shutdown_flag.is_set():
                    self.logger.info("Stop event set on global level, breaking the loop")
                    break
                if supervisor_signal_shutdown_flag.is_set():
                    self.logger.info("Stop event set by Supervisor, breaking the loop and reconnecting")
                    self._restart_stream_listener(stream_type, pairs, market)
                    break
                if on_error_shutdown_flag.is_set():
                    self.logger.info("On error on_error_shutdown_flag is set, breaking the loop and reconnecting")
                    break
                time.sleep(1)

        except Exception as e:
            self.logger.error(f"Exception {e}: Restarting {stream_type} stream of {pairs} in {market}")
            self.logger.info(f"sending _reconnect, except Exception")

        finally:
            websocket.close()
            self.logger.info(f"Ended _stream_listener on {market} {stream_type} of {pairs} in ")
            # print(f'is websocket alive: {websocket_thread.is_alive()}')

    def _restart_stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market, lag: int = 5) -> None:
        self.logger.info(f"_reconnect invocation: reconnecting in {lag} seconds on: "
                         f"{stream_type} stream of {pairs} in {market}")
        time.sleep(lag)
        self.executor.submit(self._stream_listener, stream_type, pairs, market)

    def _stream_writer(self, market: Any, duration: int, dump_path: str, stream_type: Any) -> None:
        queues = {
            StreamType.TRANSACTIONS: self.transaction_stream_message_queue,
            StreamType.ORDERBOOK: self.orderbook_stream_message_queue
        }

        queue = queues.get(stream_type, None)

        while not self.global_shutdown_flag.is_set():
            self._process_stream_data(queue, market, dump_path, stream_type)
            self._sleep_with_flag_check(duration)

        self._process_stream_data(queue, market, dump_path, stream_type)
        self.logger.info('ended _stream_writer')

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 2
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_stream_data(self, queue, market, dump_path, stream_type):
        if not queue.empty():
            stream_data = defaultdict(list)

            while not queue.empty():
                try:
                    # self.logger.info(f'queue.qsize() {queue.qsize()}')
                    message, timestamp = queue.get_nowait()
                    message = json.loads(message)
                    message["E"] = timestamp

                    stream = message["stream"]
                    stream_data[stream].append(message)
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")

            for stream, data in stream_data.items():
                _pair = stream.split('@')[0]
                file_name = self.get_file_name(_pair, market, stream_type)

                file_path = os.path.join(dump_path, file_name)
                # self.logger.info(f'Saving data to: {file_path}')

                try:
                    with open(file_path, 'w') as f:
                        json.dump(data, f)
                except IOError as e:
                    self.logger.error(f"IO error when writing to file {file_path}: {e}")

    def _snapshot_daemon(self, pairs: List[str], market: Market, dump_path: str, interval: int) -> None:

        while not self.global_shutdown_flag.is_set():
            for pair in pairs:
                try:
                    snapshot, request_timestamp, receive_timestamp = self._get_snapshot(pair, market)
                    snapshot['_rq'] = request_timestamp
                    snapshot['_rc'] = receive_timestamp

                    file_name = self.get_file_name(pair=pair, market=market, stream_type=StreamType.ORDERBOOK_SNAPSHOT)
                    full_path = os.path.join(dump_path, file_name)
                    with open(full_path, 'w') as f:
                        json.dump(snapshot, f)

                except Exception as e:
                    self.logger.error(f'error whilst fetching {pair} {market} {StreamType.ORDERBOOK_SNAPSHOT}: {e}')

            time.sleep(interval)

        self.logger.info('snapshot daemon has ended')

    def _get_snapshot(self, pair: str, market: Market) -> Tuple[Dict[str, Any], int, int] | None:
        url = URLFactory.get_snapshot_url(market=market, pair=pair)

        try:
            request_timestamp = self._get_utc_timestamp_epoch_milliseconds()
            response = requests.get(url, timeout=5)
            receive_timestamp = self._get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()
            data = response.json()

            return data, request_timestamp, receive_timestamp

        except requests.Timeout as e:
            raise f'Timeout occurred while fetching {pair} in {market}: {e}'
        except requests.RequestException as e:
            raise RuntimeError(f"Request failed: {e}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON decode failed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred: {e}") from e

    def _zip_daemon(self, file_name: str, dump_path: str = '') -> None:

        zip_file_name = file_name + '.zip'
        zip_path = f'{dump_path}/{zip_file_name}'
        file_path = f'{dump_path}/{file_name}'

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(file_path, arcname=file_name.split('/')[-1])

        if self.remove_csv_after_zip:
            os.remove(file_path)

        if self.send_zip_to_blob:
            self.upload_file_to_blob(zip_path, zip_file_name)

    def upload_file_to_blob(self, file_path: str, blob_name: str) -> None:
        """
        Uploads a file to an Azure Blob Storage container and optionally removes the file locally.

        :param file_path: The full path to the file that needs to be uploaded.
        :param blob_name: The name of the blob within the Azure Blob Storage container where the file will be stored.
                          This is effectively the file name in the cloud.

        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        if self.remove_zip_after_upload:
            os.remove(file_path)

    def get_file_name(self, pair: str, market: Market, stream_type: StreamType, extension: str = 'json') -> str:

        pair_lower = pair.lower()
        formatted_now_timestamp = self._get_utc_formatted_timestamp()

        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            StreamType.ORDERBOOK: 'binance_l2lob_raw_delta_broadcast',
            StreamType.TRANSACTIONS: 'binance_transaction_broadcast',
            StreamType.ORDERBOOK_SNAPSHOT: 'binance_l2lob_snapshot'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(stream_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.{extension}'

    @staticmethod
    def _get_utc_formatted_timestamp() -> str:
        return datetime.utcnow().strftime('%d-%m-%YT%H-%M-%SZ')

    @staticmethod
    def _get_utc_timestamp_epoch_milliseconds() -> int:
        return round(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def _get_utc_timestamp_epoch_seconds() -> int:
        return round(datetime.now(timezone.utc).timestamp())
