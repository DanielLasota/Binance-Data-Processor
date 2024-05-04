import asyncio
import json
import os
import threading
import time
import zipfile
from datetime import datetime
from typing import Optional, Any
import aiofiles
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
            should_zip_be_removed_after_upload: bool = True
    ) -> None:
        self.should_csv_be_removed_after_zip = should_csv_be_removed_after_zip
        self.should_zip_be_removed_after_upload = should_zip_be_removed_after_upload
        self.lock: threading.Lock = threading.Lock()
        self.last_file_change_time = datetime.now()
        self.blob_service_client = BlobServiceClient.from_connection_string(azure_blob_parameters_with_key)
        self.container_name = container_name
        self.message_queue = Queue()

    def run(
            self,
            instrument: str,
            market: Market,
            single_file_listen_duration_in_seconds: int,
            dump_path: str = None
    ) -> None:
        thread: threading.Thread = threading.Thread(
            target=self.listener,
            args=(instrument, market)
        )
        thread.daemon = True
        thread.start()

        file_thread: threading.Thread = threading.Thread(
            target=self.file_writer,
            args=(market, instrument, single_file_listen_duration_in_seconds, dump_path)
        )
        file_thread.daemon = True
        file_thread.start()

    def listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            self._listener(instrument, market)
        )
        loop.close()

    async def _listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        while True:
            try:
                url = self.get_stream_url(market, instrument)
                async with connect(url) as websocket:
                    while True:
                        data = await websocket.recv()
                        with self.lock:
                            self.message_queue.put(data)
                        print(data)
            except WebSocketException as e:
                print(f"WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}. Attempting to restart listener...")
                time.sleep(1)

    def file_writer(
            self,
            market: Market,
            instrument: str,
            single_file_listen_duration_in_seconds: int,
            dump_path: str
    ) -> None:
        while True:
            if not self.message_queue.empty():
                snapshot_url = self.get_snapshot_url(market=market, pair=instrument, limit=5000)
                snapshot_file_name = self.get_file_name(instrument, market, '.json')

                stream_file_name = self.get_file_name('instrument', market, '.csv')
                self.launch_snapshot_fetcher(snapshot_url, snapshot_file_name, dump_path)

                time.sleep(single_file_listen_duration_in_seconds)
                with open(f'{dump_path}{stream_file_name}', 'a') as f:
                    while not self.message_queue.empty():
                        data = self.message_queue.get()
                        f.write(f'{data},\n')

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
            file_name = f'snapshot_{file_name}'
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
        csv_path = f'{dump_path}{file_name}'

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(csv_path, arcname=file_name.split('/')[-1])

        if self.should_csv_be_removed_after_zip:
            os.remove(csv_path)

        self.upload_file_to_blob(zip_path, zip_file_name)

    def upload_file_to_blob(self, file_path: str, blob_name: str):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        if self.should_zip_be_removed_after_upload:
            os.remove(file_path)

        # print(f"Uploaded {blob_name} to blob storage.")

    @staticmethod
    def get_snapshot_url(
            market: Market,
            pair: str,
            limit: int = 1000
    ) -> Optional[str]:
        return {
            Market.SPOT: f'https://api.binance.com/api/v3/depth?symbol={pair}&limit={limit}',
            Market.USD_M_FUTURES: f'https://fapi.binance.com/fapi/v1/depth?symbol={pair}&limit={limit}',
            Market.COIN_M_FUTURES: f'https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_200925&limit={limit}'
        }.get(market, None)

    @staticmethod
    def get_stream_url(market, pair):
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@depth@100ms',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@depth@100ms',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams=btcusd_200925@depth.'
        }.get(market, None)

    @staticmethod
    def get_file_name(
            instrument: str,
            market: Market,
            extension
    ) -> str:
        pair_lower = instrument.lower()
        now = datetime.now()
        formatted_now_timestamp = now.strftime('%d-%m-%YT%H-%M-%S')

        market_short_name = None

        match market:
            case Market.SPOT:
                market_short_name = 'spot'
            case Market.USD_M_FUTURES:
                market_short_name = 'futures_usd_m'
            case Market.COIN_M_FUTURES:
                market_short_name = 'futures_coin_m'

        file_name = f'L2lob_raw_delta_broadcast_{formatted_now_timestamp}_{market_short_name}_{pair_lower}{extension}'
        return file_name
