import asyncio
import threading
import time
from datetime import datetime
from typing import Optional, Any
import aiofiles
from websockets import connect, WebSocketException

from abstract_base_classes.observer import Observer
from .market_enum import Market


class OrderbookDaemon(Observer):
    def __init__(self) -> None:
        super().__init__()
        self.lock: threading.Lock = threading.Lock()
        self.orderbook_message: Optional[dict] = None
        self.formatted_target_orderbook: Optional[Any] = None
        self.last_file_change_time = datetime.now()
        self.file_name = ""

    @staticmethod
    def get_file_name(pair_lower: str, market: Market) -> str:
        now = datetime.now()
        formatted_now_timestamp = now.strftime('%d-%m-%Y-%H-%M-%S')

        market_short_name = None

        match market:
            case Market.SPOT:
                market_short_name = 'spot'
            case Market.USD_M_FUTURES:
                market_short_name = 'usdm'
            case Market.COIN_M_FUTURES:
                market_short_name = 'coinm'

        file_name = f'level_2_lob_delta_broadcast_{formatted_now_timestamp}_{market_short_name}_{pair_lower}.csv'
        return file_name

    async def async_listener(
            self,
            instrument: str,
            market: Market
    ):
        while True:
            try:
                pair_lower = instrument.lower()
                url = None
                match market:
                    case Market.SPOT:
                        url = f'wss://stream.binance.com:9443/ws/{pair_lower}@depth@100ms'
                    case Market.USD_M_FUTURES:
                        url = f'wss://fstream.binance.com/stream?streams={pair_lower}@depth@100ms'
                    case Market.COIN_M_FUTURES:
                        url = f'wss://dstream.binance.com/stream?streams=btcusd_200925@depth.'

                self.file_name = self.get_file_name(pair_lower, market)

                async with connect(url) as websocket:
                    while True:
                        data = await websocket.recv()
                        with self.lock:
                            self.orderbook_message = data
                        print(data)

                        if (datetime.now() - self.last_file_change_time).total_seconds() >= 60:
                            self.file_name = self.get_file_name(pair_lower, market)
                            self.last_file_change_time = datetime.now()

                        async with aiofiles.open(file=self.file_name, mode='a') as f:
                            await f.write(f'{data}\n')

            except WebSocketException as e:
                print(f"WebSocket error: {e}. Reconnecting...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}. Attempting to restart listener...")
                time.sleep(1)

    def listener(
            self,
            instrument: str,
            market: Market
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_listener(instrument, market))
        loop.close()

    def run(
            self,
            instrument: str,
            market: Market,

    ) -> None:
        thread: threading.Thread = threading.Thread(
            target=self.listener,
            args=(instrument, market)
        )
        thread.daemon = True
        thread.start()
