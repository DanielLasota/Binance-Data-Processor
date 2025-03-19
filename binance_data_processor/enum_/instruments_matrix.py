from dataclasses import dataclass
from binance_data_processor.enum_.market_enum import Market
import pprint

@dataclass
class InstrumentsMatrix:
    spot: list[str] | None = None
    usd_m_futures: list[str] | None = None
    coin_m_futures: list[str] | None = None

    def __post_init__(self):
        self.dict: dict[Market, list[str]] = {}
        if self.spot is not None and self.spot:
            self.dict[Market.SPOT] = self.spot
        if self.usd_m_futures is not None and self.usd_m_futures:
            self.dict[Market.USD_M_FUTURES] = self.usd_m_futures
        if self.coin_m_futures is not None and self.coin_m_futures:
            self.dict[Market.COIN_M_FUTURES] = self.coin_m_futures

    def __str__(self) -> str:
        return pprint.pformat(self.dict, indent=1)

    def add_pair(self, market: Market, pair: str) -> None:
        if market not in self.dict:
            self.dict[market] = []
        if pair not in self.dict[market]:
            self.dict[market].append(pair)
        else:
            raise Exception(f"Instrument '{pair}' already exists in market '{market.name}'")

    def remove_pair(self, market: Market, instrument: str) -> None:
        if market in self.dict:
            if instrument in self.dict[market]:
                self.dict[market].remove(instrument)
            else:
                raise Exception(f'There is no instrument {instrument} subscribed in market {market}')
        else:
            raise Exception(f"Market '{market.name}' does not exist.")

    def get_pairs(self, market: Market) -> list[str]:
        return self.dict.get(market, [])

    def is_pair(self, market: Market, instrument: str) -> bool:
        instruments = self.dict.get(market, [])
        return instrument in instruments