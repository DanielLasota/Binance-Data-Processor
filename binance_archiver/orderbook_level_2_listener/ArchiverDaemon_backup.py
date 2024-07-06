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

from . import SupervisedQueue
from .SupervisedQueue import SupervisedQueue
from .market_enum import Market
import requests
from websocket import WebSocketApp, ABNF
from concurrent.futures import ThreadPoolExecutor

from .stream_age_enum import StreamAge
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
        self.global_shutdown_flag = threading.Event()
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.orderbook_stream_message_queue = SupervisedQueue()
        self.transaction_stream_message_queue = SupervisedQueue()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.debug_list = []

    def run(
            self,
            pairs: List[str],
            market: Market,
            file_duration_seconds: int,
            dump_path: str | None = None,
            websockets_lifetime_seconds: int = 60 * 60 * 8,
            websocket_overlap_seconds: int = 60,
            save_to_json: bool = False,
            save_to_zip: bool = False,
            send_zip_to_blob: bool = True
    ) -> None:
        # self.logger.info(f'launching daemon: '
        #                  f'{market} {pairs} {file_duration_seconds}s path: {dump_path}')

        self.start_orderbook_stream_listener(pairs, market, websockets_lifetime_seconds, websocket_overlap_seconds)
        self.start_orderbook_stream_writer(market, file_duration_seconds, dump_path, save_to_json, save_to_zip,send_zip_to_blob)
        # self.start_transaction_stream_listener(pairs, market, websockets_lifetime_seconds, websocket_overlap_seconds)
        # self.start_transaction_stream_writer(market, file_duration_seconds, dump_path, save_to_json, save_to_zip, send_zip_to_blob)
        # self.start_snapshot_daemon(pairs, market, dump_path, self.snapshot_fetcher_interval_seconds, save_to_json, save_to_zip, send_zip_to_blob)

    def start_orderbook_stream_listener(self, pairs: List[str], market: Market, lifetime: int, overlap: int) -> None:
        time.sleep(overlap + 10)
        self.executor.submit(self._stream_listener, StreamType.DIFFERENCE_DEPTH, pairs, market, lifetime, overlap)

    def start_orderbook_stream_writer(self, market, file_duration_seconds, dump_path, save_to_json, save_to_zip,
                                      send_zip_to_blob) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.DIFFERENCE_DEPTH,
                             save_to_json, save_to_zip, send_zip_to_blob)

    def start_transaction_stream_listener(self, pairs: List[str], market: Market, lifetime: int, overlap: int) -> None:
        time.sleep(overlap + 10)
        self.executor.submit(self._stream_listener, StreamType.TRADE, pairs, market, lifetime, overlap)

    def start_transaction_stream_writer(self, market, file_duration_seconds, dump_path, save_to_json, save_to_zip,
                                        send_zip_to_blob) -> None:
        self.executor.submit(self._stream_writer, market, file_duration_seconds, dump_path, StreamType.TRADE,
                             save_to_json, save_to_zip, send_zip_to_blob)

    def start_snapshot_daemon(self, pairs, market, dump_path, interval, save_to_json, save_to_zip, send_zip_to_blob):
        self.executor.submit(self._snapshot_daemon, pairs, market, dump_path, interval, save_to_json, save_to_zip,
                             send_zip_to_blob)

    def _stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market, websocket_lifetime: int,
                         overlap: int) -> None:

        stream_age = StreamAge.NEW

        _listener_specs = {
            'stream_age': stream_age,
            'stream_type': stream_type,
        }

        supervisor_signal_shutdown_flag = threading.Event()
        on_error_shutdown_flag = threading.Event()

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH: URLFactory.get_orderbook_stream_url,
            StreamType.TRADE: URLFactory.get_transaction_stream_url
        }

        queues = {
            StreamType.TRADE: self.transaction_stream_message_queue,
            StreamType.DIFFERENCE_DEPTH: self.orderbook_stream_message_queue
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)
        queue = queues.get(stream_type, None)

        supervisor = Supervisor(
            logger=self.logger,
            stream_type=stream_type,
            market=market,
            on_error_callback=lambda: supervisor_signal_shutdown_flag.set()
        )

        def _on_message(ws, message):
            _timestamp = self._get_utc_timestamp_epoch_milliseconds()

            # self.logger.info(f"{market} {stream_type}: {message}")

            with self.lock:
                queue.put_with_no_repetitions(message=message, received_timestamp=_timestamp)
                supervisor.notify()

        def _on_error(ws, error):
            on_error_shutdown_flag.set()

        def _on_close(ws, close_status_code, close_msg):
            self.logger.info(f"{market} {stream_type}: WebSocket connection closed, "
                             f"{close_msg} (code: {close_status_code})")
            supervisor.shutdown_supervisor()
            websocket_app.is_closed = True
            ws.close()

        def _on_ping(ws, message):
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            self.logger.info(f"{market} {stream_type}: WebSocket connection opened")
            self.logger.info(f'###{market}: len: {len(self.executor._threads)}')

        websocket_app = None

        def _close_websocket_after_lifetime(ws, websocket_lifetime_, overlap_):
            time.sleep(websocket_lifetime_ - overlap_)
            self.logger.info(f'{market} {stream_type}: Websocket lifetime will run out in: '
                             f'overlap ({overlap_} seconds), launching new stream listener')
            self._restart_stream_listener(stream_type, pairs, market, websocket_lifetime, overlap_)

            time.sleep(overlap_)

            if ws.keep_running:
                self.logger.info(f'{market} {stream_type}: Websocket lifetime ended after overlap')
                ws.close()

        try:
            websocket_app = WebSocketApp(
                url,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
                on_ping=_on_ping,
                on_open=_on_open
            )

            websocket_app.is_closed = False

            websocket_thread = threading.Thread(target=websocket_app.run_forever)
            websocket_thread.start()

            lifetime_thread = threading.Thread(target=_close_websocket_after_lifetime,
                                               args=(websocket_app, websocket_lifetime, overlap))
            lifetime_thread.start()

            while websocket_app.is_closed is False:
                if self.global_shutdown_flag.is_set():
                    self.logger.warning("Stop event set on global level global_shutdown_flag is set, breaking the loop")
                    break
                if supervisor_signal_shutdown_flag.is_set():
                    self.logger.error(f"{market} {stream_type}: "
                                      f"Stop event set by Supervisor, breaking the loop and reconnecting")
                    self._restart_stream_listener(stream_type, pairs, market, websocket_lifetime, overlap)
                    break
                if on_error_shutdown_flag.is_set():
                    self.logger.warning(f"{market} {stream_type}: "
                                        f"On error on_error_shutdown_flag is set, breaking the loop and reconnecting")
                    break
                time.sleep(1)

        except Exception as e:
            self.logger.error(f"{market} {stream_type}: Exception {e}: "
                              f"restarting listener")

        finally:
            websocket_app.close()
            websocket_app.is_closed = True
            self.logger.info(f"{market} {stream_type}: finally ended _stream_listener")

    def _restart_stream_listener(self, stream_type: StreamType, pairs: List[str], market: Market,
                                 websocket_lifetime: int, overlap: int) -> None:
        self.logger.info(f"{market} {stream_type}: "
                         f"new stream listener invocation: _restart_stream_listener: ")

        self.executor.submit(self._stream_listener, stream_type, pairs, market, websocket_lifetime, overlap)

    def _stream_writer(self, market: Any, duration: int, dump_path: str, stream_type: Any, save_to_json, save_to_zip,
                       send_zip_to_blob) -> None:
        queues = {
            StreamType.TRADE: self.transaction_stream_message_queue,
            StreamType.DIFFERENCE_DEPTH: self.orderbook_stream_message_queue
        }

        queue = queues.get(stream_type, None)

        while not self.global_shutdown_flag.is_set():
            self._process_stream_data(queue, market, dump_path, stream_type, save_to_json, save_to_zip,
                                      send_zip_to_blob)
            self._sleep_with_flag_check(duration)

        self._process_stream_data(queue, market, dump_path, stream_type, save_to_json, save_to_zip, send_zip_to_blob)
        self.logger.info(f'{market} {stream_type}: ended _stream_writer')

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 2
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _process_stream_data(self, queue: SupervisedQueue, market: Market, dump_path: str, stream_type: StreamType,
                             save_to_json: bool, save_to_zip: bool, send_zip_to_blob: bool) -> None:
        if not queue.empty():
            stream_data = defaultdict(list)

            while not queue.empty():
                try:
                    message, timestamp = queue.get_nowait()
                    message = json.loads(message)
                    message["_e"] = timestamp

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
                    self._save_to_json(data, file_path)

                if save_to_zip is True:
                    self._save_to_zip(data, file_name, file_path)

                if send_zip_to_blob is True:
                    self._send_zipped_json_to_blob(data, file_name)

    def _save_to_json(self, data, file_path) -> None:
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f)
        except IOError as e:
            self.logger.error(f"IO error when writing to file {file_path}: {e}")

    def _save_to_zip(self, data, file_name, file_path):
        zip_file_path = f"{file_path}.zip"
        try:
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                json_data = json.dumps(data)
                json_filename = f"{file_name}.json"
                zipf.writestr(json_filename, json_data)
        except IOError as e:
            self.logger.error(f"IO error when writing to zip file {zip_file_path}: {e}")

    def _send_zipped_json_to_blob(self, data, file_name):
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

    def _snapshot_daemon(self, pairs: List[str], market: Market, dump_path: str, interval: int, save_to_json: bool,
                         save_to_zip: bool, send_zip_to_blob: bool) -> None:

        while not self.global_shutdown_flag.is_set():
            for pair in pairs:
                try:
                    snapshot, request_timestamp, receive_timestamp = self._get_snapshot(pair, market)
                    snapshot['_rq'] = request_timestamp
                    snapshot['_rc'] = receive_timestamp

                    file_name = self.get_file_name(pair=pair, market=market, stream_type=StreamType.DEPTH_SNAPSHOT)
                    file_path = os.path.join(dump_path, file_name)

                    if save_to_json is True:
                        self._save_to_json(snapshot, file_path)

                    if save_to_zip is True:
                        self._save_to_zip(snapshot, file_name, file_path)

                    if send_zip_to_blob is True:
                        self._send_zipped_json_to_blob(snapshot, file_name)

                except Exception as e:
                    self.logger.error(f'error whilst fetching snapshot: {market} {StreamType.DEPTH_SNAPSHOT}: {e}')

            time.sleep(interval)

        self.logger.info(f'{market}: snapshot daemon has ended')

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
            StreamType.DIFFERENCE_DEPTH: 'binance_l2lob_delta_broadcast',
            StreamType.DEPTH_SNAPSHOT: 'binance_l2lob_snapshot',
            StreamType.TRADE: 'binance_transaction_broadcast'
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
