from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import traceback
import websockets
from collections import deque

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.frames import Frame, OP_TEXT, OP_BINARY, OP_CONT

from binance_data_processor.core.shared_ring_buffer import SharedRingBuffer
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.core.url_factory import URLFactory
from binance_data_processor.utils.time_utils import round_epoch_ns_to_ms
from binance_data_processor.utils.time_utils import round_epoch_ns_to_us

def _make_epoch_ns_reader():
    if sys.platform != "win32":
        return time.time_ns

    import ctypes
    from ctypes import wintypes

    #todo: w calym pliku dopisac obsluge windows/linux
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    get_time = kernel32.GetSystemTimePreciseAsFileTime
    get_time.argtypes = [ctypes.POINTER(wintypes.FILETIME)]
    get_time.restype = None

    filetime = wintypes.FILETIME()
    filetime_ptr = ctypes.pointer(filetime)

    # Różnica epok 1601-01-01 → 1970-01-01, w jednostkach 100 ns.
    epoch_offset_100ns = 116444736000000000

    def epoch_ns() -> int:
        get_time(filetime_ptr)
        ticks = ((filetime.dwHighDateTime << 32) | filetime.dwLowDateTime)
        return (ticks - epoch_offset_100ns) * 100

    return epoch_ns


class TimestampedConnection(ClientConnection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._epoch_ns = _make_epoch_ns_reader()
        self._batch_ns: int | None = None
        self._message_timestamps: deque[int] = deque()

    def data_received(self, data: bytes) -> None:
        self._batch_ns = self._epoch_ns()

        super().data_received(data)

    def process_event(self, event) -> None:
        if isinstance(event, Frame) and event.opcode in (OP_TEXT, OP_BINARY, OP_CONT) and event.fin:
            if self._batch_ns is None:
                raise RuntimeError("Missing receive timestamp")

            self._message_timestamps.append(self._batch_ns)

        super().process_event(event)

    async def recv_timestamped(self) -> tuple[str, int]:
        msg = await super().recv(decode=True)
        return msg, self._message_timestamps.popleft()


class Client:
    __slots__ = ('logger', 'client_to_collector_srb', 'ap', 'cid', 'task', '_stop_event', '_ws_lock', '_ws', '_url', '_round_ts')

    def __init__(self, ap: AssetParams, cid: bool):
        self.logger = logging.getLogger('binance_data_sink')
        srb_prefix = ("dq" if ap.stream_type is StreamType.DIFFERENCE_DEPTH_STREAM else "tq")
        self.client_to_collector_srb = SharedRingBuffer.attach(f"client_to_collector_{srb_prefix}_{ap.market.name}", 1 << 22)
        self.ap = ap
        self.cid = cid
        self.task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._ws_lock = asyncio.Lock()
        self._ws: TimestampedConnection | None = None
        self._url = URLFactory.get_stream_url(ap)
        self._round_ts = (round_epoch_ns_to_us if ap.market is Market.SPOT else round_epoch_ns_to_ms)

    async def run(self):
        self.logger.info(f"{self.ap.market.name} {self.ap.stream_type.name} Starting")

        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._main_coroutine())

    async def close(self):
        self.logger.info(f"{self.ap.market.name} {self.ap.stream_type.name} Closing")
        self._stop_event.set()

        async with self._ws_lock:
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:
                    ...

        if self.task:
            try:
                await self.task
            except Exception:
                ...

        self.task = None

    async def _main_coroutine(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                        self._url,
                        create_connection=TimestampedConnection,
                        ping_interval=None,
                        ping_timeout=None,
                        max_size=None,
                        max_queue=None,
                        write_limit=(2 ** 30, 2 ** 30 - 1)
                ) as ws:
                    async with self._ws_lock:
                        self._ws = ws

                    await self._recv_loop(ws)

            except (OSError, websockets.exceptions.ConnectionClosed) as e:
                self.logger.info(f"Reconnecting {self.ap}: {e}")

            except Exception as e:
                self.logger.info(f"Unexpected error {self.ap}: {e}\n{traceback.format_exc()}")

            finally:
                async with self._ws_lock:
                    self._ws = None

    async def _recv_loop(self, ws: TimestampedConnection):
        round_ts = self._round_ts

        recv_timestamped = ws.recv_timestamped
        srb_put_nowait = self.client_to_collector_srb.put_nowait
        cid = self.cid

        while not self._stop_event.is_set():
            try:
                msg, ns = await recv_timestamped()
                ts = round_ts(ns)

                if "stream" not in msg:
                    continue

                srb_put_nowait(msg, ts, cid)

            except ConnectionClosed as e:
                if not self._stop_event.is_set():
                    self.logger.info( "ConnectionClosed %s %s: %s", self.ap, self.cid, e,)
                break

    async def update_subscription(self, action: str, pair: str):
        method = {
            "subscribe": "SUBSCRIBE",
            "unsubscribe": "UNSUBSCRIBE",
        }.get(action.lower())

        if not method:
            return

        if self.ap.stream_type is StreamType.TRADE_STREAM:
            params = [f"{pair.lower()}@trade"]

        elif self.ap.stream_type is StreamType.DIFFERENCE_DEPTH_STREAM:
            params = [f"{pair.lower()}@depth@100ms"]

        else:
            return

        async with self._ws_lock:
            if not self._ws:
                return
            ws = self._ws

        try:
            await ws.send(json.dumps({"method": method, "params": params, "id": 1,}))

        except Exception as e:
            self.logger.info( "Error sending subscription: %s", e)
