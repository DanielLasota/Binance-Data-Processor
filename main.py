import time

from orderbook.orderbook_level_2_daemon import Level2OrderbookDaemon
from orderbook.market_enum import Market

manager = Level2OrderbookDaemon()
manager.run(
    instrument='BTCUSDT',
    market=Market.SPOT,
    single_file_listen_duration_in_seconds=600
)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
