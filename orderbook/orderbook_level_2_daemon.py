import asyncio
import threading
from datetime import datetime
from typing import Optional, Any
import aiofiles
from websockets import connect

from abstract_base_classes.observer import Observer


class OrderbookDaemon(Observer):
    def __init__(self) -> None:
        super().__init__()
        self.lock: threading.Lock = threading.Lock()
        self.orderbook_message: Optional[dict] = None
        self.formatted_target_orderbook: Optional[Any] = None
        self.last_file_change_time = datetime.now()
        self.file_name = ""

    @staticmethod
    def get_file_name(pair_lower: str) -> str:
        now = datetime.now()
        formatted_now = now.strftime('%d-%m-%Y-%H-%M-%S')
        file_name = f'level_2_lob_delta_broadcast_{formatted_now}_{pair_lower}.csv'
        return file_name

    async def async_listener(self, instrument: str):
        pair_lower = instrument.lower()
        url = f'wss://stream.binance.com:9443/ws/{pair_lower}@depth@100ms'
        today = datetime.now().date()
        self.file_name = self.get_file_name(pair_lower)

        async with connect(url) as websocket:
            while True:
                data = await websocket.recv()
                with self.lock:
                    self.orderbook_message = data
                print(data)

                if (datetime.now() - self.last_file_change_time).total_seconds() >= 60:
                    self.file_name = self.get_file_name(pair_lower)
                    self.last_file_change_time = datetime.now()

                async with aiofiles.open(file=self.file_name, mode='a') as f:
                    await f.write(f'{data}\n')

    def listener(self, instrument: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_listener(instrument))
        loop.close()

    def run(self, instrument: str) -> None:
        thread: threading.Thread = threading.Thread(
            target=self.listener,
            args=(instrument,)
        )

        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    daemon = OrderbookDaemon()
    daemon.run('BTCUSDT')
