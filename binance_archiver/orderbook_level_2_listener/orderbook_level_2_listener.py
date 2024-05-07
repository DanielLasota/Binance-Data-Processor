import json
import os
import threading
import time
import zipfile
from datetime import datetime
from websockets import WebSocketException
from azure.storage.blob import BlobServiceClient
from .market_enum import Market
import requests
from queue import Queue
from websocket import WebSocketException, WebSocket
from concurrent.futures import ThreadPoolExecutor
from .url_factory import URLFactory


class ArchiverDaemon:
    def __init__(
            self,
            logger,
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
        :param remove_zip_after_upload: Boolean flag to determine if ZIP files should be deleted after uploading to Azure Blob Storage.
        :param send_zip_to_blob: Boolean flag to determine if ZIP files should be uploaded to Azure Blob Storage.

        Attributes:
            lock (threading.Lock): A threading lock to manage access to shared resources in a thread-safe manner.
            last_file_change_time (datetime): Timestamp of the last file operation, used to monitor and log file changes.
            blob_service_client (BlobServiceClient): Client for interacting with Azure Blob Storage, initialized with the provided connection string.
            container_name (str): Container name in Azure Blob Storage for storing files.
            orderbook_stream_message_queue (Queue): Queue for storing messages from the orderbook data stream.
            transaction_stream_message_queue (Queue): Queue for storing messages from the transaction data stream.
            executor (ThreadPoolExecutor): Executor with a configurable number of workers to manage concurrent tasks.

        The executor uses a maximum of 6 workers by default, which can be adjusted based on workload requirements.
        """
        self.logger = logger
        self.should_csv_be_removed_after_zip = remove_csv_after_zip
        self.should_zip_be_removed_after_upload = remove_zip_after_upload
        self.should_zip_be_sent = send_zip_to_blob
        self.shutdown_flag = shutdown_flag
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

        Note: If `dump_path` is not specified, ensure that the default path is correctly set up to avoid any file handling errors.
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
        """
        A generic stream listener for both orderbook and transaction streams.
        :param stream_type: 'orderbook' or 'transaction' to specify the type of stream.
        :param instrument: Trading pair or instrument identifier.
        :param market: Market enum indicating the specific market.
        """
        url_method = URLFactory.get_orderbook_stream_url if stream_type == 'orderbook' \
            else URLFactory.get_transaction_stream_url
        queue = self.orderbook_stream_message_queue if stream_type == 'orderbook' \
            else self.transaction_stream_message_queue

        while not self.shutdown_flag.is_set():
            websocket = WebSocket()
            try:
                url = url_method(market, instrument)
                websocket.connect(url)
                while not self.shutdown_flag.is_set():
                    data = websocket.recv()
                    with self.lock:
                        queue.put(data)
            except WebSocketException as e:
                self.logger.info(f"{stream_type.capitalize()} WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                self.logger.info(f"Unexpected error in {stream_type} stream: {e}. Attempting to restart listener...")
                time.sleep(1)
            finally:
                websocket.close()

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
        :param duration: The interval in seconds between writing data to the file system. This controls how often the data
                         is dumped from the queue to the filesystem.
        :param dump_path: The directory path where the resulting files will be stored.
        :param stream_type: A string indicating the type of data stream ('transaction' or 'orderbook'). This determines which
                            queue to monitor and the specific processing logic to apply.

        The function operates in an infinite loop, checking if the corresponding data queue is not empty,
        and processing the data accordingly. If the stream type is 'orderbook', it additionally fetches
        a snapshot from a URL and saves it separately.

        Files are appended with new data at each interval, and once written, the file archiving process is triggered to
        optionally compress and upload the data to Azure Blob Storage.

        Note: This method should run in a dedicated thread or asynchronous task since it contains a blocking infinite loop
        and time-delayed operations.
        """
        queue = self.transaction_stream_message_queue if stream_type == 'transaction' \
            else self.orderbook_stream_message_queue
        limits = {Market.SPOT: 5000, Market.USD_M_FUTURES: 1000, Market.COIN_M_FUTURES: 1000}

        while not self.shutdown_flag.is_set():
            if not queue.empty():
                file_name = self.get_file_name(instrument, market, f'{stream_type}_stream', 'json')

                if stream_type == 'orderbook':
                    limit = limits.get(market, 1000)
                    snapshot_url = URLFactory.get_snapshot_url(market=market, pair=instrument, limit=limit)
                    snapshot_file_name = (
                        self.get_file_name(instrument, market, 'orderbook_snapshot', 'json'))
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

        After fetching the data and writing it to a file, the method calls `launch_zip_daemon` to handle possible
        zipping and uploading of the file based on class configurations.

        Raises:
            Exception: An exception is raised if the HTTP request does not return a 200 (OK) status code, indicating
                       that the data fetch was unsuccessful. The exception message includes the failed status code.

        Note:
            This method includes a delay (sleep) of 1 second before executing the request to mitigate the risk of
            hitting API rate limits or to handle use cases that require a slight delay for any reason.
        """
        time.sleep(1)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            full_path = os.path.join(dump_path, file_name)
            with open(full_path, 'w') as f:
                json.dump(data, f, indent=4)
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

        This method takes a file specified by `file_name` within the `dump_path` directory, compresses it into a ZIP file,
        and optionally handles the deletion of the original file and the uploading of the ZIP file based on the daemon's configuration.

        :param file_name: The name of the file to be compressed. This file must exist within the directory specified by `dump_path`.
                          The '.zip' extension is automatically appended to this name for the created ZIP file.
        :param dump_path: The path to the directory where the file resides and where the ZIP file will be created.
                          If not specified, it defaults to the current working directory.

        The method performs the following operations:
        - Creates a ZIP file in the specified `dump_path` with maximum compression.
        - If enabled (`should_csv_be_removed_after_zip`), deletes the original file after successful compression.
        - If enabled (`should_zip_be_sent`), uploads the newly created ZIP file to Azure Blob Storage.

        Note:
            - The method assumes that the path management (e.g., ensuring the directory exists) is handled externally.
            - Proper permissions must be in place to read the source file, write the ZIP file, and delete the source file if configured.
            - The method does not return any value but will raise filesystem or network exceptions if operations fail.
        """

        zip_file_name = file_name + '.zip'
        zip_path = f'{dump_path}/{zip_file_name}'
        file_path = f'{dump_path}/{file_name}'

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(file_path, arcname=file_name.split('/')[-1])

        if self.should_csv_be_removed_after_zip:
            os.remove(file_path)

        if self.should_zip_be_sent:
            self.upload_file_to_blob(zip_path, zip_file_name)

    def upload_file_to_blob(
            self,
            file_path: str,
            blob_name: str
    ) -> None:
        """
        Uploads a file to an Azure Blob Storage container and optionally removes the file locally.

        This method is responsible for uploading a specified file to Azure Blob Storage using the provided blob name.
        It opens the file in binary read mode and uploads its contents to the blob storage. After successfully uploading,
        the method can optionally delete the local file based on the class configuration.

        :param file_path: The full path to the file that needs to be uploaded. This file must exist on the local file system.
        :param blob_name: The name of the blob within the Azure Blob Storage container where the file will be stored.
                          This is effectively the file name in the cloud.

        Upon successful upload, if the configuration `should_zip_be_removed_after_upload` is set to True,
        the method removes the local file to free up space or clean up the local storage.

        Note:
            - This method requires that the Azure storage client is properly configured with the correct access keys and
              permissions.
            - The method assumes the `self.blob_service_client` and `self.container_name` are correctly set up during the
              instantiation of the class.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        if self.should_zip_be_removed_after_upload:
            os.remove(file_path)

    @staticmethod
    def get_timestamp():
        now = datetime.now()
        return now.strftime('%d-%m-%YT%H-%M-%S')

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

        :param instrument: The trading instrument identifier (e.g., 'BTCUSD'). The instrument name is converted to lowercase.
        :param market: The market type, which should be an enum value from Market (e.g., Market.SPOT, Market.USD_M_FUTURES).
        :param data_type: A string representing the type of data (e.g., 'orderbook_stream', 'transaction_stream').
        :param extension: The file extension (e.g., 'json', 'csv') to append to the file name.
        :return: A string representing the formatted file name.

        The method maps the market and data type to predefined strings that describe these aspects more verbosely.
        If no matching key is found in the mapping for market or data type, 'unknown_market' or 'unknown_data_type'
        is used respectively. The file name includes a timestamp in the format '%d-%m-%YT%H-%M-%S' to prevent overwriting
        previous files and to track data chronologically.

        Example of generated file name:
            'l2lob_raw_delta_broadcast_spot_btcusd_05-10-2023T14-30-01.json'

        Note:
            This method uses `ArchiverDaemon.get_timestamp()` to generate the timestamp, assuming this method is defined
            elsewhere in the ArchiverDaemon class that returns the current date and time formatted as a string.
        """

        pair_lower = instrument.lower()
        formatted_now_timestamp = ArchiverDaemon.get_timestamp()

        market_mapping = {
            Market.SPOT: 'spot',
            Market.USD_M_FUTURES: 'futures_usd_m',
            Market.COIN_M_FUTURES: 'futures_coin_m'
        }

        data_type_mapping = {
            'orderbook_stream': 'l2lob_raw_delta_broadcast',
            'transaction_stream': 'transaction_broadcast',
            'orderbook_snapshot': 'l2lob_snapshot'
        }

        market_short_name = market_mapping.get(market, 'unknown_market')
        prefix = data_type_mapping.get(data_type, 'unknown_data_type')

        return f'{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.{extension}'
