import threading
from logging import Logger
from typing import List, Tuple
import time
from websocket import WebSocketApp, ABNF

from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType
from binance_archiver.orderbook_level_2_listener.supervisor import Supervisor
from binance_archiver.orderbook_level_2_listener.url_factory import URLFactory


class StreamListener:
    def __init__(self):
        self.stream_type: StreamType | None = None
        self.shutdown_flag: threading.Event = threading.Event()
        self.id: StreamId = StreamId()
        self.pairs_amount = None

    def run_listener(self, queue, pairs: List[str], stream_type: StreamType, market: Market, logger: Logger) -> None:

        self.stream_type = stream_type

        supervisor_signal_shutdown_flag = threading.Event()

        supervisor = Supervisor(
            logger=logger,
            stream_type=stream_type,
            market=market,
            check_interval_in_seconds=5,
            max_interval_without_messages_in_seconds=10,
            on_error_callback=lambda: supervisor_signal_shutdown_flag.set()
        )

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH: URLFactory.get_orderbook_stream_url,
            StreamType.TRADE: URLFactory.get_transaction_stream_url
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_message(ws, message):
            logger.info(f"{self.id.start_timestamp} {market} {stream_type}: {message}")
            self.id.pairs_amount = len(pairs)
            queue.put(stream_listener_id=self.id, message=message)
            supervisor.notify()

        def _on_error(ws, error):
            logger.error(f"{market} {stream_type}: {error}")

        def _on_close(ws, close_status_code, close_msg):
            logger.info(f"{market} {stream_type}: WebSocket connection closed, "
                        f"{close_msg} (code: {close_status_code})")
            supervisor.shutdown_supervisor()
            ws.close()

        def _on_ping(ws, message):
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            logger.info(f"{market} {stream_type}: WebSocket connection opened")

        websocket_app = WebSocketApp(
            url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
            on_ping=_on_ping,
            on_open=_on_open
        )

        websocket_thread = threading.Thread(target=websocket_app.run_forever)
        websocket_thread.start()

        while True:
            if self.shutdown_flag.is_set():
                logger.info("shutdown_flag is set, breaking the loop")
                websocket_app.close()
                break

            if supervisor_signal_shutdown_flag.is_set():
                logger.error(f"{market} {stream_type}: "
                             f"Stop event set by Supervisor, breaking the loop and reconnecting")
                websocket_app.close()
                break

            time.sleep(1)

    def end(self):
        self.shutdown_flag.set()
