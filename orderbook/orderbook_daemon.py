import threading
from typing import Any, Optional
from binance import ThreadedWebsocketManager

from abstract_base_classes.observer import Observer


class OrderbookDaemon(Observer):
    def __init__(self) -> None:
        super().__init__()
        self.lock: threading.Lock = threading.Lock()
        self.orderbook_message: Optional[dict] = None
        self.formatted_target_orderbook: Optional[Any] = None

    def orderbook_listener(
            self,
            instrument: str,
            api_key: str,
            api_secret: str
    ) -> None:
        def handle_orderbook(message: dict) -> None:
            with self.lock:
                self.orderbook_message = message
                print(message)

        twm: ThreadedWebsocketManager = ThreadedWebsocketManager(api_key, api_secret)
        twm.start()

        twm.start_depth_socket(
            callback=handle_orderbook,
            symbol=instrument,
            depth='20',
            interval=100
        )

        twm.join()

    def run(
            self,
            instrument: str,
            api_key: str,
            secret_key: str
    ) -> None:
        thread: threading.Thread = threading.Thread(
            target=self.orderbook_listener,
            args=(
                instrument,
                api_key,
                secret_key
            )
        )

        thread.daemon = True
        thread.start()
