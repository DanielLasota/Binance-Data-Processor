import time

from orderbook.orderbook_level_2_daemon import OrderbookDaemon
from orderbook.market_enum import Market

manager = OrderbookDaemon()
manager.run(
    instrument='BTCUSDT',
    market=Market.USD_M_FUTURES
)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
