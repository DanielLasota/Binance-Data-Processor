from enum import IntEnum, auto


class Symbol(IntEnum):
    def _generate_next_value_(name, start, count, last_values):
        return count

    UNKNOWN = auto()
    BTCUSDT = auto()
    ETHUSDT = auto()
    BNBUSDT = auto()
    SOLUSDT = auto()
    XRPUSDT = auto()
    DOGEUSDT = auto()
    ADAUSDT = auto()
    SHIBUSDT = auto()
    LTCUSDT = auto()
    AVAXUSDT = auto()
    TRXUSDT = auto()
    DOTUSDT = auto()
    BCHUSDT = auto()
    SUIUSDT = auto()


def normalize_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith('_PERP'):
        s = s.replace('_PERP', 'T')
    return s

def symbol_to_int(symbol: str) -> int:
    return Symbol[normalize_symbol(symbol)].value
