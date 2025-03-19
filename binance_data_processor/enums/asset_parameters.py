from __future__ import annotations

from dataclasses import dataclass

from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


@dataclass(slots=True)
class AssetParameters:
    market: Market
    stream_type: StreamType
    pairs: list[str]
    date: str | None = None

    def __str__(self):
        return (
            f""
            f"{self.stream_type.name} "
            f"{self.market.name} "
            f"{self.pairs[0].upper() if len(self.pairs) == 1 else self.pairs} "
            f"{self.date if not None else ''}"
        )

    def get_asset_parameter_with_specified_pair(self, pair: str) -> AssetParameters:
        return AssetParameters(
            market=self.market,
            stream_type=self.stream_type,
            pairs=[pair]
        )
