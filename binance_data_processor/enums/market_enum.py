from enum import Enum


class Market(Enum):
    UNKNOWN = 'unknown'
    SPOT = 'spot'
    USD_M_FUTURES = 'usd_m_futures'
    COIN_M_FUTURES = 'coin_m_futures'
    ...

M_STREAM_CODE = {
    Market.UNKNOWN:         0,
    Market.SPOT:            1,
    Market.USD_M_FUTURES:   2,
    Market.COIN_M_FUTURES:  3,
}
