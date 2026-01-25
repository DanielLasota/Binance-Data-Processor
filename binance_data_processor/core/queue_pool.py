from __future__ import annotations

from queue import Queue

from binance_data_processor import DataSinkCfg
from binance_data_processor.core.difference_depth_queue import DifferenceDepthQueue
from binance_data_processor.core.trade_queue import TradeQueue
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


class QueuePool:
    __slots__ = [
        'cfg',
        'global_queue',
        'queues'
    ]

    def __init__(self, cfg: DataSinkCfg, global_queue: Queue | None = None):
        self.cfg = cfg
        self.global_queue = global_queue

        self.queues: dict[tuple[Market, StreamType], DifferenceDepthQueue | TradeQueue] = {
            (market, stream_type): (
                DifferenceDepthQueue if stream_type == StreamType.DIFFERENCE_DEPTH_STREAM
                else TradeQueue
            )(
                market=market,
                **({"global_queue": self.global_queue} if self.global_queue else {})
            )
            for market in Market if market is not Market.UNKNOWN
            for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]
        }

    def get_queue(self, market: Market, stream_type: StreamType) -> DifferenceDepthQueue | TradeQueue:
        return self.queues.get((market, stream_type))


class ListenerQueuePool(QueuePool):
    def __init__(self, cfg: DataSinkCfg):
        super().__init__(cfg, global_queue=Queue())


class DataSinkQueuePool(QueuePool):
    def __init__(self, cfg: DataSinkCfg):
        super().__init__(cfg)
