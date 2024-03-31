import time

from orderbook.orderbook_level_2_daemon import OrderbookDaemon


manager = OrderbookDaemon()
manager.run('BTCUSDT')

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
