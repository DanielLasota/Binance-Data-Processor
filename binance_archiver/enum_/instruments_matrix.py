import logging
from dataclasses import dataclass, field
from binance_archiver.enum_.market_enum import Market
import pprint


@dataclass
class InstrumentsMatrix:
    spot: list[str] = field(default_factory=list)
    usd_m_futures: list[str] = field(default_factory=list)
    coin_m_futures: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.dict: dict[Market, list[str]] = {
            Market.SPOT: self.spot,
            Market.USD_M_FUTURES: self.usd_m_futures,
            Market.COIN_M_FUTURES: self.coin_m_futures
        }

    def __str__(self) -> str:
        return pprint.pformat(self.dict, indent=1)

    def add_pair(
            self,
            market: Market,
            pair: str
    ) -> None:
        logger = logging.getLogger('binance_archiver')
        if market not in self.dict:
            self.dict[market] = []
        if pair not in self.dict[market]:
            self.dict[market].append(pair)
            logger.info(f"Added instrument '{pair}' to market '{market.name}'.")
        else:
            logger.info(f"Instrument '{pair}' already exists in market '{market.name}'.")

    def remove_pair(
            self,
            market: Market,
            instrument: str
    ) -> None:
        logger = logging.getLogger('binance_archiver')

        if market in self.dict:
            try:
                self.dict[market].remove(instrument)
                logger.info(f"Deleted instrument '{instrument}' from market '{market.name}'.")
            except ValueError:
                logger.info(f"Instrument '{instrument}' not found in market '{market.name}'.")
        else:
            logger.info(f"Market '{market.name}' does not exist.")


    def get_pairs(self, market: Market) -> list[str]:
        return self.dict.get(market, [])

    def is_pair(self, market: Market, instrument: str) -> bool:
        instruments = self.dict.get(market, [])
        exists = instrument in instruments
        return exists
