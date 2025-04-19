import asyncio
import json
import logging
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler
import os
import signal
import websockets

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.core.difference_depth_queue import DifferenceDepthQueue
from binance_data_processor.core.trade_queue import TradeQueue
from binance_data_processor.core.stream_listener_id import StreamListenerId
from binance_data_processor.core.url_factory import URLFactory


class StreamListener:
    __slots__ = [
        'logger',
        'queue',
        'asset_parameters',
        'id',
        'thread',
        '_stop_event',
        '_ws_lock',
        '_ws',
        '_url',
        '_loop'
    ]

    def __init__(
        self,
        queue: TradeQueue | DifferenceDepthQueue,
        asset_parameters: AssetParameters
    ):
        self.logger = logging.getLogger('binance_data_sink')

        logging.basicConfig(
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            level=logging.DEBUG,
        )

        class SkipTextFrames(logging.Filter):
            import re
            _re = re.compile(r"(^<\s+TEXT\b)|\bPUT\b")

            def filter(self, record: logging.LogRecord) -> bool:
                return not self._re.match(record.getMessage())

        console_formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03dZ %(levelname)s -- %(message)s",
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        file_handler = RotatingFileHandler(
            filename="archiver.log",
            maxBytes=10*1024*1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(console_formatter)
        _lg = logging.getLogger("websockets.client")
        _lg.setLevel(logging.DEBUG)
        _lg.addFilter(SkipTextFrames())
        _lg.addHandler(file_handler)

        self.queue: DifferenceDepthQueue | TradeQueue = queue
        self.asset_parameters = asset_parameters
        self.id: StreamListenerId = StreamListenerId(pairs=self.asset_parameters.pairs)
        self.thread: threading.Thread | None = None

        self._stop_event = threading.Event()
        self._ws_lock = threading.Lock()
        self._ws: websockets.ClientConnection | None = None
        self._url = URLFactory.get_stream_url(asset_parameters)

    def start_websocket_app(self):
        self.logger.info(f"{self.asset_parameters.market} {self.asset_parameters.stream_type} {self.id.start_timestamp} Starting streamListener")
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()

    def restart_websocket_app(self):
        self.logger.info(f"{self.asset_parameters.market} {self.asset_parameters.stream_type} {self.id.start_timestamp} Restarting streamListener")
        self.close_websocket_app()
        self._stop_event.clear()
        time.sleep(1)
        self.start_websocket_app()

    def close_websocket_app(self):
        self.logger.info(f"{self.asset_parameters.market} {self.asset_parameters.stream_type} {self.id.start_timestamp} Closing StreamListener")

        self._stop_event.set()
        with self._ws_lock:
            if self._ws:
                try:
                    asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
                except Exception as e:
                    self.logger.exception(f"Error while closing the websocket: {e}")

        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def change_subscription(self, pair: str, action: str):
        pair = pair.lower()
        method = None
        if action.lower() == "subscribe":
            method = "SUBSCRIBE"
        elif action.lower() == "unsubscribe":
            method = "UNSUBSCRIBE"

        if not method:
            self.logger.error(f"Unknown action: {action}, skipping subscription change.")
            return

        message = {}
        if self.asset_parameters.stream_type == StreamType.TRADE_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@trade"],
                "id": 1
            }
        elif self.asset_parameters.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM:
            message = {
                "method": method,
                "params": [f"{pair}@depth@100ms"],
                "id": 1
            }
            self.queue.update_deque_max_len(len(self.asset_parameters.pairs))

        loop = getattr(self, '_loop', None)
        if loop is not None and loop.is_running():
            def _do_send():
                asyncio.create_task(self._send_message(json.dumps(message)))
            loop.call_soon_threadsafe(_do_send)
        else:
            self.logger.error(f"Loop is not running, cannot {action} for pair={pair}.")

    def _run_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        try:
            loop.run_until_complete(self._main_coroutine())
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    async def _main_coroutine(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                        self._url,
                        ping_interval=None,
                        ping_timeout=None,
                        max_size=None,
                        max_queue=None,
                        write_limit=(2 ** 30, 2 ** 30 - 1)
                ) as ws:
                    with self._ws_lock:
                        self._ws = ws

                    await self._listen_messages(ws)

            except (OSError, websockets.exceptions.ConnectionClosed) as e:
                self.logger.error(f"Connection error/reconnect for {self.asset_parameters}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error in _main_coroutine: {self.asset_parameters} {e}")
                self.logger.error(traceback.format_exc())
            finally:
                with self._ws_lock:
                    self._ws = None

    async def _listen_messages(self, ws: websockets.ClientConnection):

        while not self._stop_event.is_set():
            try:
                message = await ws.recv()

                raw_timestamp_of_receive_ns = time.time_ns()
                timestamp_of_receive_rounded = (
                    (raw_timestamp_of_receive_ns + 500) // 1_000
                    if self.asset_parameters.market is Market.SPOT
                    else (raw_timestamp_of_receive_ns + 500_000) // 1_000_000
                )

                self._handle_incoming_message(
                    raw_message=message,
                    timestamp_of_receive=timestamp_of_receive_rounded
                )

            except websockets.exceptions.ConnectionClosed as e:
                if not self._stop_event.is_set():
                    self.logger.info(
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"websockets.exceptions.ConnectionClosed: {e} \n"
                        f"stream_listener_id: {self.id.id_keys} "
                        f"{self.asset_parameters.market} {self.asset_parameters.stream_type} "
                        f"self._stop_event.is_set(): {self._stop_event.is_set()}"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                        f"###############################################"
                    )
                    self.logger.error(traceback.format_exc())

                    # -- DEBUG: wewnętrzna kolejka przychodzących wiadomości ---
                    try:
                        internal_queue = getattr(ws, 'messages', None)  # to asyncio.Queue przechowujące przychodzące wiadomości :contentReference[oaicite:0]{index=0}
                        if internal_queue is not None and hasattr(internal_queue, 'qsize'):
                            queue_size = internal_queue.qsize()  # aktualna liczba elementów w kolejce :contentReference[oaicite:1]{index=1}
                            queue_limit = getattr(internal_queue, 'maxsize', None)  # maksymalny rozmiar kolejki (max_queue) :contentReference[oaicite:2]{index=2}
                        else:
                            queue_size = None
                            queue_limit = None

                        self.logger.error(
                            f"[DEBUG] WebSocket incoming queue size: {queue_size}/{queue_limit}"
                        )
                        if queue_limit is not None and queue_size is not None and queue_size >= queue_limit:
                            self.logger.error("[DEBUG] Incoming messages queue is full or exceeded max_queue limit!")
                        else:
                            self.logger.error("[DEBUG] Incoming messages queue within limits.")
                    except Exception as ie:
                        self.logger.error(f"[DEBUG] Nie udało się odczytać kolejki WebSocket: {ie}")
                    # -- DEBUG: bufor zapisu i porównanie z write_limit ---
                    try:
                        transport = ws.transport
                        # ile jest danych w buforze do wysłania
                        write_buffer_size = transport.get_write_buffer_size()  # :contentReference[oaicite:0]{index=0}
                        # próby pobrania progów (low, high)
                        try:
                            low_water, high_water = transport.get_write_buffer_limits()  # :contentReference[oaicite:1]{index=1}
                        except Exception:
                            # jeżeli niedostępne, bierzemy write_limit z obiektu ws
                            high_water = getattr(ws, 'write_limit', None)         # :contentReference[oaicite:2]{index=2}
                            low_water = None

                        self.logger.error(
                            f"[DEBUG] Write buffer size: {write_buffer_size} B; "
                            f"write_limit (high watermark): {high_water}"
                        )
                        if isinstance(high_water, int) and write_buffer_size > high_water:
                            self.logger.error("[DEBUG] Przekroczono write_limit!")
                        else:
                            self.logger.error("[DEBUG] write_buffer w normie")
                    except Exception as ie:
                        self.logger.error(f"[DEBUG] Błąd przy odczycie bufora zapisu: {ie}")

                threading.Timer(1, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
                time.sleep(1)
                threading.Timer(1, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
                break

    def _handle_incoming_message(self, raw_message: str, timestamp_of_receive: int):
        # self.logger.info(f"self.id.start_timestamp: {self.id.start_timestamp} {raw_message}")

        if 'stream' in raw_message:
            if self.asset_parameters.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM:
                self.queue.put_difference_depth_message(
                    stream_listener_id=self.id,
                    message=raw_message,
                    timestamp_of_receive=timestamp_of_receive
                )
            elif self.asset_parameters.stream_type == StreamType.TRADE_STREAM:
                self.queue.put_trade_message(
                    stream_listener_id=self.id,
                    message=raw_message,
                    timestamp_of_receive=timestamp_of_receive
                )

    async def _send_message(self, message: str):
        with self._ws_lock:
            if not self._ws:
                self.logger.error("Cannot send message – websocket is None.")
                return
            ws = self._ws

        try:
            await ws.send(message)
        except Exception as e:
            self.logger.error(f"Error while sending message: {e}")
