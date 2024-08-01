import threading
from typing import List
from websocket import WebSocketApp, ABNF

from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType
from binance_archiver.orderbook_level_2_listener.supervisor import Supervisor
from binance_archiver.orderbook_level_2_listener.url_factory import URLFactory


class StreamListener:
    def __init__(self):
        self.stream_type: StreamType | None = None
        self.shutdown_flag: bool = False
        self.id: StreamId = StreamId()
        self.pairs_amount = None
        print('_constructed new StreamListener')

    def run_listener(self, queue, pairs: List[str], stream_type: StreamType, market: Market) -> None:
        print('_invocated run_listener')
        self.stream_type = stream_type

        # supervisor_signal_shutdown_flag = threading.Event()

        # print('_setting up supervisor')
        # supervisor = Supervisor(
        #     logger=logger,
        #     stream_type=stream_type,
        #     market=market,
        #     check_interval_in_seconds=5,
        #     max_interval_without_messages_in_seconds=10,
        #     on_error_callback=lambda: supervisor_signal_shutdown_flag.set()
        # )
        # print('_supervisor fully set up')

        stream_url_methods = {
            StreamType.DIFFERENCE_DEPTH: URLFactory.get_orderbook_stream_url,
            StreamType.TRADE: URLFactory.get_transaction_stream_url
        }

        url_method = stream_url_methods.get(stream_type, None)
        url = url_method(market, pairs)

        def _on_message(ws, message):
            # print(f"{self.id.start_timestamp} {market} {stream_type}: {message}")
            self.id.pairs_amount = len(pairs)
            queue.put_message(stream_listener_id=self.id, message=message)
            # supervisor.notify()

        def _on_error(ws, error):
            print(f"_on_error: {market} {stream_type}: {error}")

        def _on_close(ws, close_status_code, close_msg):
            print(f"_on_close{market} {stream_type}: WebSocket connection closed, "
                  f"{close_msg} (code: {close_status_code})")
            # supervisor.shutdown_supervisor()
            ws.close()

        def _on_ping(ws, message):
            ws.send("", ABNF.OPCODE_PONG)

        def _on_open(ws):
            print(f"{market} {stream_type}: WebSocket connection opened")

        print('_setting up websocket app')
        websocket_app = WebSocketApp(
            url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
            on_ping=_on_ping,
            on_open=_on_open
        )
        print('_websocket app fully set up')

        print('_websocket thread is fully set up, starting websocket thread...')

        websocket_thread = threading.Thread(target=websocket_app.run_forever)
        websocket_thread.start()

        print('_started websocket thread')

        # while True:
        #     if self.shutdown_flag.is_set():
        #         print("shutdown_flag is set, breaking the loop")
        #         websocket_app.close()
        #         break
        #     #
        #     # if supervisor_signal_shutdown_flag.is_set():
        #     #     logger.error(f"{market} {stream_type}: "
        #     #                  f"Stop event set by Supervisor, breaking the loop and reconnecting")
        #     #     websocket_app.close()
        #     #     break
        #
        #     # time.sleep(1)

        while self.shutdown_flag is False:
            x=1
            # time.sleep(0.1)

        print('shutdown flag is set, ending websocket thread...  ')

        websocket_app.close()
        # websocket_thread.join()

        print('_ended websocket thread')

    def end(self):
        self.shutdown_flag = True
