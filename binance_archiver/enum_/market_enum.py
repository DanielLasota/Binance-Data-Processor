from enum import Enum


class Market(Enum):
    SPOT = 'spot'
    USD_M_FUTURES = 'usd_m_futures'
    COIN_M_FUTURES = 'coin_m_futures'
    ...
