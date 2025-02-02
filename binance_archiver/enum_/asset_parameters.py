from __future__ import annotations

from dataclasses import dataclass

from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType


@dataclass(slots=True)
class AssetParameters:
    market: Market
    stream_type: StreamType
    pairs: list[str]

    def get_asset_parameter_with_specified_pair(self, pair: str) -> AssetParameters:
        return AssetParameters(
            market=self.market,
            stream_type=self.stream_type,
            pairs=[pair]
        )
