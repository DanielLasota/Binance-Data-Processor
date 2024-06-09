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
import io

from .UniqueQueue import UniqueQueue
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
            snapshot_fetcher_interval_seconds: int = 20
    ) -> None:
        self.snapshot_fetcher_interval_seconds = snapshot_fetcher_interval_seconds
        self.logger = logger
        # self.remove_csv_after_zip = remove_csv_after_zip
        # self.remove_zip_after_upload = remove_zip_after_upload
        # self.send_zip_to_blob = send_zip_to_blob
        self.global_shutdown_flag = threading.Event()
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.orderbook_stream_message_queue = UniqueQueue()
        self.transaction_stream_message_queue = UniqueQueue()
        self.executor = ThreadPoolExecutor(max_workers=6)

    def run(
            self,
            pairs: List[str],
            market: Market,
            file_duration_seconds: int,
            dump_path: str | None = None,
            websockets_lifetime: int = 60*60*8,
            overlap: int = 60,
            save_to_json: bool = False,
            save_to_zip: bool = True,
            send_zip_to_blob: bool = True
    ) -> None:
        self.logger.info(f'launching new daemon: '
                         f'{market} {pairs} {file_duration_seconds}s path: {dump_path}')

        self.start_orderbook_stream_listener(pairs, market, websockets_lifetime, overlap)
        self.start_orderbook_stream_writer(market, file_duration_seconds, dump_path, save_to_json, save_to_zip, send_zip_to_blob)
        self.start_transaction_stream_listener(pairs, market, websockets_lifetime, overlap)
        self.start_transaction_stream_writer(market, file_duration_seconds, dump_path)
        self.start_snapshot_daemon(pairs, market, dump_path, self.snapshot_fetcher_interval_seconds)

    def start_orderbook_stream_listener(self, pairs, market, lifetime: int, overlap: int) -> None:
        self.executor.submit(self._stream_listener, StreamType.ORDERBOOK, pairs, market, lifetime, overlap)

    def start_transaction_stream_listener(self, pairs, market: Market, lifetime: int, overlap: int) -> None:
        self.executor.submit(self._stream_listener, StreamType.TRANSACTIONS, pairs, market, lifetime, overlap)

    def start_orderbook_stream_writer(self, market, file_duration_seconds, dump_path, save_to_json, save_to_zip, send_zip_to_blob) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.ORDERBOOK, save_to_json, save_to_zip, send_zip_to_blob)

    def start_transaction_stream_writer(self, market, file_duration_seconds, dump_path) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.TRANSACTIONS)

    def start_snapshot_daemon(self, pairs, market, dump_path, interval):
        self.executor.submit(self._snapshot_daemon, pairs, market, dump_path, interval)

    def _stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market, websocket_lifetime: int, overlap: int) -> None:
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

            with self.lock:
                queue.put_with_no_repetitions(message=message, received_timestamp=_timestamp)
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

        def _close_websocket_after_lifetime(ws, websocket_lifetime_, overlap_):
            time.sleep(websocket_lifetime_ - overlap_)

            self._restart_stream_listener(stream_type, pairs, market, websocket_lifetime, overlap_)

            time.sleep(overlap_)

            if ws.keep_running:
                # self.logger.info(f"WebSocket connection closed after "
                #                  f"{websocket_lifetime_} seconds for {market}, {stream_type}, {pairs}")
                ws.close()

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

            lifetime_thread = threading.Thread(target=_close_websocket_after_lifetime, args=(websocket, websocket_lifetime, overlap))
            lifetime_thread.start()

            while True:
                if self.global_shutdown_flag.is_set():
                    self.logger.info("Stop event set on global level, breaking the loop")
                    break
                if supervisor_signal_shutdown_flag.is_set():
                    self.logger.info("Stop event set by Supervisor, breaking the loop and reconnecting")
                    self._restart_stream_listener(stream_type, pairs, market, websocket_lifetime, overlap)
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

    def _restart_stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market, websocket_lifetime: int, overlap: int) -> None:
        self.logger.info(f"new stream listener invocation: _restart_stream_listener: "
                         f"{stream_type} stream of {pairs} in {market}")
        self.executor.submit(self._stream_listener, stream_type, pairs, market, websocket_lifetime, overlap)

    def _stream_writer(self, market: Any, duration: int, dump_path: str, stream_type: Any, save_to_json, save_to_zip, send_zip_to_blob) -> None:
        queues = {
            StreamType.TRANSACTIONS: self.transaction_stream_message_queue,
            StreamType.ORDERBOOK: self.orderbook_stream_message_queue
        }

        queue = queues.get(stream_type, None)

        while not self.global_shutdown_flag.is_set():
            self._process_stream_data(queue, market, dump_path, stream_type, save_to_json, save_to_zip, send_zip_to_blob)
            self._sleep_with_flag_check(duration)

        self._process_stream_data(queue, market, dump_path, stream_type, save_to_json, save_to_zip, send_zip_to_blob)
        self.logger.info('ended _stream_writer')

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 2
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_stream_data(self, queue, market, dump_path, stream_type, save_to_json, save_to_zip, send_zip_to_blob):
        if not queue.empty():
            stream_data = defaultdict(list)

            while not queue.empty():
                try:
                    # self.logger.info(f'queue.qsize() {queue.qsize()}')
                    message, timestamp = queue.get_nowait()
                    message = json.loads(message)
                    message["_E"] = timestamp

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

                if save_to_json is True:
                    try:
                        with open(file_path, 'w') as f:
                            json.dump(data, f)
                    except IOError as e:
                        self.logger.error(f"IO error when writing to file {file_path}: {e}")

                if save_to_zip is True:
                    zip_file_path = f"{file_path}.zip"
                    try:
                        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                            json_data = json.dumps(data)
                            json_filename = f"{file_name}.json"
                            zipf.writestr(json_filename, json_data)
                    except IOError as e:
                        self.logger.error(f"IO error when writing to zip file {zip_file_path}: {e}")

                if send_zip_to_blob is True:
                    try:
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            json_data = json.dumps(data)
                            json_filename = f"{file_name}.json"
                            zipf.writestr(json_filename, json_data)

                        zip_buffer.seek(0)

                        blob_client = self.blob_service_client.get_blob_client(
                            container=self.container_name,
                            blob=f"{file_name}.zip"
                        )

                        blob_client.upload_blob(zip_buffer, overwrite=True)
                        self.logger.info(f"Successfully uploaded {file_name}.zip to blob storage.")
                    except Exception as e:
                        self.logger.error(f"Error uploading zip to blob: {e}")

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
