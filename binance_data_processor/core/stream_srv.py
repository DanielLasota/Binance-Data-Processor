from __future__ import annotations

import asyncio
import traceback

from binance_data_processor.core.queue_flags_pool import QueueFlagsPool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.client import Client
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


class StreamService:

    __slots__ = (
        'cfg',
        'queue_flags_pool',
        'stop_flag',
        'logger',
        'clients',
        'overlap_lock',
        'is_ws_overlapping',
        'tasks',
        '_loop'
    )

    def __init__(
        self,
        cfg: DataSinkCfg,
        queue_flags_pool: QueueFlagsPool,
        stop_flag
    ):
        self.cfg = cfg
        self.queue_flags_pool: QueueFlagsPool = queue_flags_pool
        self.stop_flag = stop_flag

        self.logger = None
        self.clients: dict[tuple[Market, StreamType, str], Client | None] = {}
        self.overlap_lock: asyncio.Lock = asyncio.Lock()
        self.is_ws_overlapping = asyncio.Event()

        self.tasks: list[asyncio.Task] = [] #todo: escape async fuck that we don't like python's async
        self._loop = None

    def run(self):
        self.logger = setup_logger(dump=True)
        asyncio.run(self._run_async())

    async def _run_async(self):
        self._loop = asyncio.get_running_loop()

        for market, instruments in self.cfg.instruments.dict.items():
            for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
                ap = AssetParams(market=market, stream_type=stream_type, pairs=self.cfg.instruments.get_pairs(market))
                t = asyncio.create_task(self._stream_loop(ap, self.queue_flags_pool), name=f"stream_srv: {ap.market}/{ap.stream_type}")
                self.tasks.append(t)
        await asyncio.gather(*self.tasks)

    async def _stream_loop(self, ap: AssetParams, qfp: QueueFlagsPool) -> None:
        q_flags = qfp.get_flags(ap.market, ap.stream_type)
        did_ws_switch = q_flags.did_ws_switch
        set_switching_ws_mode = q_flags.set_switching_ws_mode

        async def sleep_with_flag_check(duration: int) -> None:
            interval = 1
            for _ in range(0, int(duration), interval):
                if self.stop_flag.is_set():
                    break
                await asyncio.sleep(interval)

        while not self.stop_flag.is_set():
            new_client = None
            old_client = None

            try:
                old_client = Client(ap, False)
                self.clients[(ap.market, ap.stream_type, 'old')] = old_client

                await old_client.run()
                new_client = None

                while not self.stop_flag.is_set():
                    await sleep_with_flag_check(self.cfg.time.ws_lifetime_seconds)

                    while self.is_ws_overlapping.is_set() and not self.stop_flag.is_set():
                        await asyncio.sleep(1)

                    if self.stop_flag.is_set():
                        break

                    async with self.overlap_lock:
                        self.is_ws_overlapping.set()
                        self.logger.info(f'{ap.market.name} {ap.stream_type.name} started changing ws')

                        new_client = Client(ap, not old_client.cid)
                        await new_client.run()

                        set_switching_ws_mode.set()
                        self.clients[(ap.market, ap.stream_type, 'new')] = new_client

                    while not did_ws_switch.is_set() and not self.stop_flag.is_set():
                        await asyncio.sleep(1)

                    async with self.overlap_lock:
                        self.is_ws_overlapping.clear()

                    self.logger.info(f"{ap.market.name} {ap.stream_type.name} listeners overlapped")

                    if not self.stop_flag.is_set():
                        did_ws_switch = False #todo

                        await old_client.close()
                        old_client = new_client

                        self.clients[(ap.market, ap.stream_type, 'new')] = None
                        self.clients[(ap.market, ap.stream_type, 'old')] = old_client

            except Exception as e:
                self.logger.error(f'{e}, something bad happened, Traceback:{traceback.format_exc()}')

            finally:
                for _ in (old_client, new_client):
                    if _:
                        try:
                            await _.close()
                        except Exception:
                            ...

    async def update_subscriptions(self, action: str, market: Market, pair: str) -> None:
        #todo: outdated
        for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
            for status in ['old', 'new']:
                client: Client = self.clients.get((market, stream_type, status))
                if client:
                    await client.update_subscription(action, pair)
