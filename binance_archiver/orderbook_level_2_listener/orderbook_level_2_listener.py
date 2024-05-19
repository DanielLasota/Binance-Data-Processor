import json
import logging
import os
import threading
import time
import zipfile
from datetime import datetime
from typing import Optional

from azure.storage.blob import BlobServiceClient

from .market_enum import Market
import requests
from queue import Queue
from websocket import WebSocket, WebSocketConnectionClosedException, WebSocketAddressException, \
    WebSocketTimeoutException
from concurrent.futures import ThreadPoolExecutor

from .NoSignalException import NoSignalException
from .supervisor import Supervisor
from .url_factory import URLFactory


class ArchiverDaemon:
    def __init__(
            self,
            logger: logging.Logger,
            azure_blob_parameters_with_key: str,
            container_name: str,
            shutdown_flag: threading.Event,
            remove_csv_after_zip: bool = True,
            remove_zip_after_upload: bool = True,
            send_zip_to_blob: bool = False,
    ) -> None:
        """
        Initializes an instance of ArchiverDaemon, which handles the archiving of data streams into Azure Blob Storage.

        This daemon manages the compression, uploading,
        and optional deletion of CSV and ZIP files generated from orderbook and transaction streams.
        It uses a ThreadPoolExecutor to handle tasks concurrently.

        :param azure_blob_parameters_with_key: Connection string for Azure Blob Storage, used to authorize and connect.
        :param container_name: Name of the Azure Blob Storage container where files will be stored.
        :param remove_csv_after_zip: Boolean flag to determine if CSV files should be deleted after zipping.
        :param remove_zip_after_upload: Boolean flag to determine if ZIP files should be deleted after uploading to blob
        :param send_zip_to_blob: Boolean flag to determine if ZIP files should be uploaded to Azure Blob Storage.
        """
        self.logger = logger
        self.remove_csv_after_zip = remove_csv_after_zip
        self.remove_zip_after_upload = remove_zip_after_upload
        self.send_zip_to_blob = send_zip_to_blob
        self.global_shutdown_flag = shutdown_flag
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.orderbook_stream_message_queue = Queue()
        self.transaction_stream_message_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=6)

    def run(
            self,
            instrument: str,
            market: Market,
            file_duration_seconds: int,
            dump_path: str = None
    ) -> None:
        """
        Starts listeners and writers for orderbook and transaction data streams.

        This function initiates streaming listeners for both orderbook and transactions
        for the specified instrument and market.
        It also handles writing the streamed data to files at specified intervals.
        The data is saved in the location specified by `dump_path`.

        :param instrument: The trading instrument symbol (e.g., 'BTCUSD') for which the streams will be listened to.
        :param market: The market type, defined in the Market class, which specifies the market to be monitored.
        :param file_duration_seconds: The time in seconds after which data from the stream buffer is written to a file.
        :param dump_path: The path to the directory where files will be saved. If not provided, a default path will be used.

        The method launches four parallel processes:
        - Listening to the orderbook stream.
        - Writing orderbook data to a file every `file_duration_secs`.
        - Listening to the transaction stream.
        - Writing transaction data to a file every `file_duration_secs`.

        """
        self.logger.info(f'launching lob, snapshots, transactions on: '
                         f'{market} {instrument} {file_duration_seconds}s path: {dump_path}')

        self.start_orderbook_stream_listener(instrument, market)
        self.start_orderbook_stream_writer(market, instrument, file_duration_seconds, dump_path)
        self.start_transaction_stream_listener(instrument, market)
        self.start_transaction_stream_writer(market, instrument, file_duration_seconds, dump_path)

    def start_orderbook_stream_listener(self, instrument, market) -> None:
        self.executor.submit(self._stream_listener, 'orderbook', instrument, market)

    def start_transaction_stream_listener(self, instrument, market: Market) -> None:
        self.executor.submit(self._stream_listener, 'transaction', instrument, market)

    def start_orderbook_stream_writer(self, market, instrument, file_duration_seconds, dump_path) -> None:
        self.executor.submit(self._stream_writer, market, instrument, file_duration_seconds, dump_path, 'orderbook')

    def start_transaction_stream_writer(self, market, instrument, file_duration_seconds, dump_path) -> None:
        self.executor.submit(self._stream_writer, market, instrument, file_duration_seconds, dump_path, 'transaction')

    def launch_snapshot_fetcher(self, snapshot_url: str, file_name: str, dump_path: str) -> None:
        self.executor.submit(self._snapshot_fetcher, snapshot_url, file_name, dump_path)

    def launch_zip_daemon(self, file_name: str, dump_path: str = None) -> None:
        self.executor.submit(self._zip_daemon, file_name, dump_path)

    def _stream_listener(self, stream_type: str, instrument: str, market: Market) -> None:
        url_method = URLFactory.get_orderbook_stream_url if stream_type == 'orderbook' \
            else URLFactory.get_transaction_stream_url

        queues = {
            'transaction': self.transaction_stream_message_queue,
            'orderbook': self.orderbook_stream_message_queue
        }

        stop_event = threading.Event()

        queue = queues[stream_type]
        websocket = WebSocket()

        supervisor = Supervisor(
            logger=self.logger,
            on_error_callback=lambda: stop_event.set(),
            stream_type=stream_type,
            instrument=instrument,
            market=market
        )

        try:
            url = url_method(market, instrument)
            websocket.connect(url, timeout=5)
            websocket.settimeout(5)

            while not self.global_shutdown_flag.is_set():
                if stop_event.is_set():
                    self.logger.info("Stop event set by Supervisor, breaking the loop.")
                    break

                try:
                    data = websocket.recv()
                    with self.lock:
                        queue.put(data)
                        supervisor.notify()

                except NoSignalException as e:
                    self.logger.warning(
                        f"WebSocketTimeoutException {e}: Reconnecting to {stream_type} stream of {instrument} in {market}")
                    self._reconnect(stream_type, instrument, market)
                    break
                except WebSocketTimeoutException as e:
                    self.logger.warning(
                        f"WebSocketTimeoutException {e}: Reconnecting to {stream_type} stream of {instrument} in {market}")
                    self._reconnect(stream_type, instrument, market)
                    break
                except WebSocketConnectionClosedException as e:
                    self.logger.warning(
                        f"WebSocketTimeoutException {e}: Reconnecting to {stream_type} stream of {instrument} in {market}")
                    self._reconnect(stream_type, instrument, market)
                    break
                except WebSocketAddressException as e:
                    self.logger.error(
                        f"WebSocketAddressException {e}: Restarting {stream_type} stream of {instrument} in {market}")
                    time.sleep(5)
                    self._reconnect(stream_type, instrument, market)
                    break
                except Exception as e:
                    self.logger.error(f"Exception {e}: Restarting {stream_type} stream of {instrument} in {market}")
                    self._reconnect(stream_type, instrument, market)
                    break

        except WebSocketTimeoutException as e:
            self.logger.warning(
                f"WebSocketTimeoutException {e}: Reconnecting to {stream_type} stream of {instrument} in {market}")
            self._reconnect(stream_type, instrument, market)

        except WebSocketConnectionClosedException as e:
            self.logger.warning(
                f"WebSocketConnectionClosedException: {e} Reconnecting to {stream_type} stream of {instrument} in {market}")
            self._reconnect(stream_type, instrument, market)

        except WebSocketAddressException as e:
            self.logger.error(
                f"WebSocketAddressException {e}: Restarting {stream_type} stream of {instrument} in {market}")
            time.sleep(5)
            self._reconnect(stream_type, instrument, market)

        except Exception as e:
            self.logger.error(f"Exception {e}: Restarting {stream_type} stream of {instrument} in {market}")
            self._reconnect(stream_type, instrument, market)

        finally:
            self.logger.info(f"Reached _stream_listener end on WebSocket for "
                             f"{stream_type} stream of {instrument} in {market}")
            websocket.close()

    def _reconnect(self, stream_type: str, instrument: str, market: Market) -> None:
        if not self.global_shutdown_flag.is_set():
            self.executor.submit(self._stream_listener, stream_type, instrument, market)
            self.logger.info(f"_reconnect invocation: "
                             f"Resubmitted task for {stream_type} stream of {instrument} in {market}")

    def _stream_writer(
            self,
            market: Market,
            instrument: str,
            duration: int,
            dump_path: str,
            stream_type: str
    ) -> None:
        """
        Handles the writing of streaming data to files for specified market and instrument,
        and manages data archiving based on stream type.

        This method continuously checks a designated queue for data and writes it to a file
        at intervals specified by the duration parameter. For orderbook streams, it also fetches
        snapshot data using the specified market and instrument.

        :param market: The market type as specified in the Market enum, which determines the source of the stream data.
        :param instrument: The trading instrument identifier (e.g., 'BTCUSD') for which the stream data is relevant.
        :param duration: The interval in seconds between writing data to the file system.
                         This controls how often the data is dumped from the queue to the filesystem.
        :param dump_path: The directory path where the resulting files will be stored.
        :param stream_type: A string indicating the type of data stream ('transaction' or 'orderbook').
                            This determines which
                            queue to monitor and the specific processing logic to apply.
        """

        queues = {
            'transaction': self.transaction_stream_message_queue,
            'orderbook': self.orderbook_stream_message_queue
        }

        try:
            queue = queues[stream_type]

        except KeyError:
            raise ValueError(f"Invalid stream type: {stream_type}")

        limits = {
            Market.SPOT: 5000,
            Market.USD_M_FUTURES: 1000,
            Market.COIN_M_FUTURES: 1000
        }

        while not self.global_shutdown_flag.is_set():
            if not queue.empty():
                file_name = self.get_file_name(instrument, market, f'{stream_type}_stream', 'json')

                if stream_type == 'orderbook':
                    limit = limits.get(market, 1000)
                    snapshot_url = URLFactory.get_snapshot_url(market=market, pair=instrument, limit=limit)
                    snapshot_file_name = (
                        self.get_file_name(instrument, market, 'orderbook_snapshot', 'json')
                    )
                    self.launch_snapshot_fetcher(snapshot_url, snapshot_file_name, dump_path)

                time.sleep(duration)

                data_list = []
                while not queue.empty():
                    data = queue.get()
                    data_list.append(json.loads(data))

                with open(os.path.join(dump_path, file_name), 'a') as f:
                    json.dump(data_list, f)

                self.launch_zip_daemon(file_name, dump_path)

    def _snapshot_fetcher(
            self,
            url: str,
            file_name: str,
            dump_path: str
    ) -> None:
        """
        Fetches data from a specified URL and writes the JSON response to a file in a specified directory.

        This method is intended to retrieve snapshot data from a given URL, typically pointing to a market data source.
        The retrieved data is saved to a JSON file in the specified dump path. If the HTTP request is successful,
        the data is saved and then the method triggers a process to possibly zip and upload the file to a remote
        storage location. If the request fails, an exception is raised.

        :param url: The complete URL from which to fetch the snapshot data. This should be a full URL string
                    that includes any necessary query parameters.
        :param file_name: The name of the file to save the fetched data to. This name should include a .json
                          extension as the data is saved in JSON format.
        :param dump_path: The path to the directory where the output file should be stored. This path must exist
                          on the file system.

        """
        time.sleep(1)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            full_path = os.path.join(dump_path, file_name)
            with open(full_path, 'w') as f:
                json.dump(data, f)
            self.launch_zip_daemon(file_name, dump_path)
        else:
            raise Exception(f"Failed to get data: {response.status_code} response")

    def _zip_daemon(
            self,
            file_name: str,
            dump_path: str = ''
    ) -> None:
        """
        Compresses a specified file into a ZIP archive and optionally uploads it to Azure Blob Storage.

        This method takes a file specified by `file_name` within the `dump_path` directory,
        compresses it into a ZIP file,
        and optionally handles the deletion of the original file
        and the uploading of the ZIP file based on the daemon's configuration.

        :param file_name: The name of the file to be compressed. This file must exist within
                          the directory specified by `dump_path`.
                          The '.zip' extension is automatically appended to this name for the created ZIP file.
        :param dump_path: The path to the directory where the file resides and where the ZIP file will be created.
                          If not specified, it defaults to the current working directory.
        """

        zip_file_name = file_name + '.zip'
        zip_path = f'{dump_path}/{zip_file_name}'
        file_path = f'{dump_path}/{file_name}'

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(file_path, arcname=file_name.split('/')[-1])

        if self.remove_csv_after_zip:
            os.remove(file_path)

        if self.send_zip_to_blob:
            self.upload_file_to_blob(zip_path, zip_file_name)

    def upload_file_to_blob(
            self,
            file_path: str,
            blob_name: str
    ) -> None:
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

    @staticmethod
    def get_utc_timestamp():
        return datetime.utcnow().strftime('%d-%m-%YT%H-%M-%SZ')

    @staticmethod
    def get_file_name(
            instrument: str,
            market: Market,
            data_type: str,
            extension: str
    ) -> str:
        """
        Generates a formatted file name based on instrument, market, data type, and file extension.

        This method constructs a file name by combining details about the trading instrument, the market,
        the type of data, and the file extension. It ensures that file names are consistent and descriptive,
        including a timestamp to make each file name unique.

        :param instrument: The trading instrument identifier (e.g., 'BTCUSD').
            The instrument name is converted to lowercase.
        :param market: The market type, which should be an enum value from Market
            (e.g., Market.SPOT, Market.USD_M_FUTURES).
        :param data_type: A string representing the type of data (e.g., 'orderbook_stream', 'transaction_stream').
        :param extension: The file extension (e.g., 'json', 'csv') to append to the file name.
        :return: A string representing the formatted file name.
        """

        pair_lower = instrument.lower()
        formatted_now_timestamp = ArchiverDaemon.get_utc_timestamp()

        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            'orderbook_stream': 'binance_l2lob_raw_delta_broadcast',
            'transaction_stream': 'binance_transaction_broadcast',
            'orderbook_snapshot': 'binance_l2lob_snapshot'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(data_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.{extension}'
