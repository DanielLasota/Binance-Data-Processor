import threading
import time
from typing import List
from websocket import WebSocketApp, ABNF

from binance_archiver.orderbook_level_2_listener.difference_depth_queue import (
    DifferenceDepthQueue,
)
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType
from binance_archiver.orderbook_level_2_listener.supervisor import Supervisor
from binance_archiver.orderbook_level_2_listener.trade_queue import TradeQueue
from binance_archiver.orderbook_level_2_listener.url_factory import URLFactory


class PairsLengthException(Exception):
    ...


class WrongListInstanceException(Exception):
    ...


class StreamListener:
    def __init__(
        self,
        queue: TradeQueue | DifferenceDepthQueue,
        pairs: List[str],
        stream_type: StreamType,
        market: Market,
    ):
        if not isinstance(pairs, list):
            raise WrongListInstanceException('pairs argument is not a list')
        if len(pairs) == 0:
            raise PairsLengthException('pairs len is zero')

        self.supervisor_signal_shutdown_flag: threading.Event = threading.Event()
        self.id: StreamId = StreamId()
        self.pairs_amount: int = len(pairs)
        self.websocket_app: WebSocketApp = self._construct_websocket_app(queue, pairs, stream_type, market)
        self.supervisor: Supervisor

    def _construct_websocket_app(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        pairs: List[str],
        stream_type: StreamType,
        market: Market,
    ) -> WebSocketApp:
        supervisor_signal_shutdown_flag = self.supervisor_signal_shutdown_flag

        self.supervisor = Supervisor(
            stream_type=stream_type,
            market=market,
            check_interval_in_seconds=5,
            max_interval_without_messages_in_seconds=10,
            on_error_callback=lambda: supervisor_signal_shutdown_flag.set()
        )

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH: URLFactory.get_orderbook_stream_url,
            StreamType.TRADE: URLFactory.get_transaction_stream_url,
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_difference_depth_message(ws, message):
            # print(f"{self.id.start_timestamp} {market} {stream_type}: {message}")
            timestamp_of_receive = int(time.time() * 1000 + 0.5)
            self.id.pairs_amount = len(pairs)
            queue.put_queue_message(stream_listener_id=self.id, message=message,
                                    timestamp_of_receive=timestamp_of_receive)
            self.supervisor.notify()

        def _on_trade_message(ws, message):
            # print(f"{self.id.start_timestamp} {market} {stream_type}: {message}")
            timestamp_of_receive = int(time.time() * 1000 + 0.5)
            self.id.pairs_amount = len(pairs)
            queue.put_trade_message(message=message, timestamp_of_receive=timestamp_of_receive)
            self.supervisor.notify()

        def _on_error(ws, error):
            print(f"_on_error: {market} {stream_type} {self.id.start_timestamp}: {error}")

        def _on_close(ws, close_status_code, close_msg):
            print(
                f"_on_close: {market} {stream_type} {self.id.start_timestamp}: WebSocket connection closed, "
                f"{close_msg} (code: {close_status_code})"
            )
            self.supervisor.shutdown_supervisor()
            # ws.close()

        def _on_ping(ws, message):
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            print(f"_on_open : {market} {stream_type} {self.id.start_timestamp}: WebSocket connection opened")

        websocket_app = WebSocketApp(
            url=url,
            on_message=(
                _on_trade_message
                if stream_type == StreamType.TRADE
                else _on_difference_depth_message
            ),
            on_error=_on_error,
            on_close=_on_close,
            on_ping=_on_ping,
            on_open=_on_open,
        )

        return websocket_app
