import asyncio
import json
import os
import threading
import time
import zipfile
from datetime import datetime
from typing import Optional
from websockets import connect, WebSocketException
from azure.storage.blob import BlobServiceClient
from .market_enum import Market
import requests
from queue import Queue


class OrderbookDaemon:
    def __init__(
            self,
            azure_blob_parameters_with_key: str,
            container_name: str,
            should_csv_be_removed_after_zip: bool = True,
            should_zip_be_removed_after_upload: bool = True,
            should_zip_be_sent: bool = True
    ) -> None:
        self.should_csv_be_removed_after_zip = should_csv_be_removed_after_zip
        self.should_zip_be_removed_after_upload = should_zip_be_removed_after_upload
        self.should_zip_be_sent = should_zip_be_sent
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.orderbook_stream_message_queue = Queue()
        self.transaction_stream_message_queue = Queue()

    def run(
            self,
            instrument: str,
            market: Market,
            single_file_listen_duration_in_seconds: int,
            dump_path: str = None
    ) -> None:
        orderbook_stream_thread: threading.Thread = threading.Thread(
            target=self.orderbook_stream_listener,
            args=(instrument, market)
        )
        orderbook_stream_thread.daemon = True
        orderbook_stream_thread.start()

        orderbook_stream_saver_thread: threading.Thread = threading.Thread(
            target=self.orderbook_stream_writer,
            args=(market, instrument, single_file_listen_duration_in_seconds, dump_path)
        )
        orderbook_stream_saver_thread.daemon = True
        orderbook_stream_saver_thread.start()

        #############

        transaction_stream_thread: threading.Thread = threading.Thread(
            target=self.transaction_stream_listener,
            args=(instrument, market)
        )
        transaction_stream_thread.daemon = True
        transaction_stream_thread.start()

        transaction_stream_writer_thread: threading.Thread = threading.Thread(
            target=self.transaction_stream_writer,
            args=(market, instrument, single_file_listen_duration_in_seconds, dump_path)
        )
        transaction_stream_writer_thread.daemon = True
        transaction_stream_writer_thread.start()

    def transaction_stream_listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            self._transaction_stream_listener(instrument, market)
        )
        loop.close()

    async def _transaction_stream_listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        while True:
            try:
                url = self.get_transaction_stream_url(market, instrument)
                async with connect(url) as websocket:
                    while True:
                        data = await websocket.recv()
                        with self.lock:
                            self.transaction_stream_message_queue.put(data)
                        # print(data)
            except WebSocketException as e:
                print(f"WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}. Attempting to restart listener...")
                time.sleep(1)

    def orderbook_stream_listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            self._orderbook_stream_listener(instrument, market)
        )
        loop.close()

    async def _orderbook_stream_listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        while True:
            try:
                url = self.get_orderbook_stream_url(market, instrument)
                async with connect(url) as websocket:
                    while True:
                        data = await websocket.recv()
                        with self.lock:
                            self.orderbook_stream_message_queue.put(data)
                        # print(data)
            except WebSocketException as e:
                print(f"WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}. Attempting to restart listener...")
                time.sleep(1)

    def transaction_stream_writer(
            self,
            market: Market,
            instrument: str,
            single_file_listen_duration_in_seconds: int,
            dump_path: str
    ) -> None:
        while True:
            if not self.transaction_stream_message_queue.empty():
                stream_file_name = (
                    self.get_file_name(instrument, market,'transaction_stream', 'json'))

                time.sleep(single_file_listen_duration_in_seconds)

                data_list = []
                while not self.transaction_stream_message_queue.empty():
                    data = self.transaction_stream_message_queue.get()
                    data_list.append(json.loads(data))

                with open(f'{dump_path}{stream_file_name}', 'a') as f:
                    json.dump(data_list, f)
                self.launch_zip_daemon(stream_file_name, dump_path)

    def orderbook_stream_writer(
            self,
            market: Market,
            instrument: str,
            single_file_listen_duration_in_seconds: int,
            dump_path: str
    ) -> None:
        while True:
            if not self.orderbook_stream_message_queue.empty():

                limit = None

                if market == Market.SPOT:
                    limit = 5000
                elif market == Market.USD_M_FUTURES:
                    limit = 1000
                elif market == Market.COIN_M_FUTURES:
                    limit = 1000

                snapshot_url = self.get_snapshot_url(market=market, pair=instrument, limit=limit)
                snapshot_file_name = (
                    self.get_file_name(instrument, market, 'orderbook_snapshot', 'json'))

                stream_file_name = (
                    self.get_file_name(instrument, market, 'orderbook_stream', 'json'))
                self.launch_snapshot_fetcher(snapshot_url, snapshot_file_name, dump_path)

                time.sleep(single_file_listen_duration_in_seconds)
                data_list = []
                while not self.orderbook_stream_message_queue.empty():
                    data = self.orderbook_stream_message_queue.get()
                    data_list.append(json.loads(data))

                with open(f'{dump_path}{stream_file_name}', 'a') as f:
                    json.dump(data_list, f)

                self.launch_zip_daemon(stream_file_name, dump_path)

    def launch_snapshot_fetcher(
            self,
            snapshot_url: str,
            file_name: str,
            dump_path: str,
    ):
        snapshot_thread = threading.Thread(
            target=self._snapshot_fetcher,
            args=(
                snapshot_url,
                file_name,
                dump_path
            )
        )

        snapshot_thread.daemon = True
        snapshot_thread.start()

    def _snapshot_fetcher(
            self,
            url: str,
            file_name: str,
            dump_path: str,
            lag_in_seconds: int = 1
    ) -> None:
        time.sleep(lag_in_seconds)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            full_path = os.path.join(dump_path, file_name)
            with open(full_path, 'w') as f:
                json.dump(data, f, indent=4)
            self.launch_zip_daemon(file_name, dump_path)
        else:
            raise Exception(f"Failed to get data: {response.status_code} response")

    def launch_zip_daemon(
            self,
            file_name: str,
            dump_path: str = None
    ) -> None:
        zip_thread = threading.Thread(target=self._zip_daemon, args=(file_name, dump_path))
        zip_thread.daemon = True
        zip_thread.start()

    def _zip_daemon(
            self,
            file_name: str,
            dump_path: str = ''
    ) -> None:

        zip_file_name = file_name + '.zip'
        zip_path = f'{dump_path}{zip_file_name}'
        file_path = f'{dump_path}{file_name}'

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(file_path, arcname=file_name.split('/')[-1])

        if self.should_csv_be_removed_after_zip:
            os.remove(file_path)

            # try:
            #     os.remove(file_path)
            # except Exception as e:
            #     print(e)

        if self.should_zip_be_sent:
            self.upload_file_to_blob(zip_path, zip_file_name)

    def upload_file_to_blob(self, file_path: str, blob_name: str):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        if self.should_zip_be_removed_after_upload:
            os.remove(file_path)

    @staticmethod
    def get_snapshot_url(
            market: Market,
            pair: str,
            limit: int
    ) -> Optional[str]:
        return {
            Market.SPOT: f'https://api.binance.com/api/v3/depth?symbol={pair}&limit={limit}',
            Market.USD_M_FUTURES: f'https://fapi.binance.com/fapi/v1/depth?symbol={pair}&limit={limit}',
            Market.COIN_M_FUTURES: f'https://dapi.binance.com/dapi/v1/depth?symbol={pair}&limit={limit}'
        }.get(market, None)

    @staticmethod
    def get_transaction_stream_url(market, pair):
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@trade',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@trade',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@trade'
        }.get(market, None)

    @staticmethod
    def get_orderbook_stream_url(market, pair):
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@depth@100ms',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@depth@100ms',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@depth'
        }.get(market, None)

    @staticmethod
    def get_file_name(
            instrument: str,
            market: Market,
            data_type: str,
            extension: str
    ) -> str:
        pair_lower = instrument.lower()
        now = datetime.now()
        formatted_now_timestamp = now.strftime('%d-%m-%YT%H-%M-%S')

        market_short_name = None
        prefix = None

        match market:
            case Market.SPOT:
                market_short_name = 'spot'
            case Market.USD_M_FUTURES:
                market_short_name = 'futures_usd_m'
            case Market.COIN_M_FUTURES:
                market_short_name = 'futures_coin_m'

        match data_type:
            case 'orderbook_stream':
                prefix = 'l2lob_raw_delta_broadcast'
            case 'transaction_stream':
                prefix = 'transaction_broadcast'
            case 'orderbook_snapshot':
                prefix = 'l2lob_snapshot'

        file_name = f'{prefix}_{market_short_name}_{pair_lower}_{formatted_now_timestamp}.{extension}'
        return file_name
