from __future__ import annotations

import multiprocessing as mp

from binance_data_processor import DataSinkCfg
from binance_data_processor.core.difference_depth_queue import DifferenceDepthQueue
from binance_data_processor.core.trade_queue import TradeQueue
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


class QueuePool:
    __slots__ = [
        'cfg',
        'global_queue',
        'queues',
        # 'cmd_queue'
    ]

    def __init__(self, cfg: DataSinkCfg, global_queue: mp.Queue | None = None):
        self.cfg = cfg
        self.global_queue = global_queue
        # self.cmd_queue = mp.Queue()

        self.queues: dict[tuple[Market, StreamType], DifferenceDepthQueue | TradeQueue] = {
            (m, st):
                (DifferenceDepthQueue if st == StreamType.DIFFERENCE_DEPTH_STREAM else TradeQueue)(
                    market=m,
                    pairs_n=len(cfg.instruments.get_pairs(m)),
                    **({"global_queue": self.global_queue} if self.global_queue else {})
                )
            for m in cfg.instruments.dict.keys()
            for st in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queues.get((market, stream_type))


class ListenerQueuePool(QueuePool):
    def __init__(self, cfg: DataSinkCfg):
        super().__init__(cfg, global_queue=mp.Queue(maxsize=100_000))


class DataSinkQueuePool(QueuePool):
    def __init__(self, cfg: DataSinkCfg):
        super().__init__(cfg)
