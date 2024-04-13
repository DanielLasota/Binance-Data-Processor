import asyncio
import os
import threading
import time
import zipfile
from datetime import datetime
from typing import Optional, Any
import aiofiles
from websockets import connect, WebSocketException

from abstract_base_classes.observer import Observer
from .market_enum import Market


class Level2OrderbookDaemon(Observer):
    def __init__(self) -> None:
        super().__init__()
        self.lock: threading.Lock = threading.Lock()
        self.orderbook_message: Optional[dict] = None
        self.formatted_target_orderbook: Optional[Any] = None
        self.last_file_change_time = datetime.now()
        self.file_name = ""

    @staticmethod
    def get_url(market: Market, pair: str):
        pair_lower = pair.lower()

        url = None
        match market:
            case Market.SPOT:
                url = f'wss://stream.binance.com:9443/ws/{pair_lower}@depth@100ms'
            case Market.USD_M_FUTURES:
                url = f'wss://fstream.binance.com/stream?streams={pair_lower}@depth@100ms'
            case Market.COIN_M_FUTURES:
                url = f'wss://dstream.binance.com/stream?streams=btcusd_200925@depth.'

        return url

    @staticmethod
    def get_file_name(instrument: str, market: Market) -> str:
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

        file_name = f'level_2_lob_raw_delta_broadcast_{formatted_now_timestamp}_{market_short_name}_{pair_lower}.csv'
        return file_name

    async def async_listener(
            self,
            instrument: str,
            market: Market,
            single_file_listen_duration_in_seconds: int,
            dump_path: str = None
    ) -> None:
        while True:
            try:
                url = self.get_url(market, instrument)
                self.file_name = self.get_file_name(instrument, market)

                if dump_path is not None:
                    dump_path = f'{dump_path}/'

                async with connect(url) as websocket:
                    while True:
                        data = await websocket.recv()
                        with self.lock:
                            self.orderbook_message = data
                        print(data)

                        if (
                                (datetime.now() - self.last_file_change_time).total_seconds() >=
                                single_file_listen_duration_in_seconds
                        ):
                            self.launch_zip_daemon(self.file_name)
                            self.file_name = self.get_file_name(instrument, market)
                            self.last_file_change_time = datetime.now()

                        async with aiofiles.open(
                                file=f'{dump_path}{self.file_name}',
                                mode='a'
                        ) as f:
                            await f.write(f'{data}\n')

            except WebSocketException as e:
                print(f"WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}. Attempting to restart listener...")
                time.sleep(1)

    def launch_zip_daemon(self, file_name):
        zip_thread = threading.Thread(target=self._zip_daemon, args=(file_name,))
        zip_thread.daemon = True
        zip_thread.start()

    @staticmethod
    def _zip_daemon(
            file_name: str,
            dump_path: str = None,
    ) -> None:

        if dump_path is not None:
            dump_path = f'{dump_path}/'

        zip_file_name = file_name.replace('.csv', '.csv.zip')
        with zipfile.ZipFile(f'{dump_path}{zip_file_name}', 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.write(f'{dump_path}{file_name}', arcname=file_name.split('/')[-1])
        os.remove(f'{dump_path}{file_name}')

    def listener(
            self,
            instrument: str,
            market: Market,
            single_file_listen_duration_in_seconds: int,
            dump_path: str = None
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(
            self.async_listener(
                instrument,
                market,
                single_file_listen_duration_in_seconds=single_file_listen_duration_in_seconds,
                dump_path=dump_path
            )
        )
        loop.close()

    def run(
            self,
            instrument: str,
            market: Market,
            single_file_listen_duration_in_seconds: int,
            dump_path: str = None
    ) -> None:
        thread: threading.Thread = threading.Thread(
            target=self.listener,
            args=(instrument, market, single_file_listen_duration_in_seconds, dump_path)
        )
        thread.daemon = True
        thread.start()
