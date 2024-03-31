from binance.client import Client
from dotenv import load_dotenv
import os
import time

from orderbook.orderbook_daemon import OrderbookDaemon


def main(name):
    load_dotenv(os.path.expandvars(r'%USERPROFILE%/creds.env'))
    api_key, api_secret = os.environ.get('API_KEY'), os.environ.get('SECRET_KEY')

    print(api_key)

    orderbook_daemon = OrderbookDaemon()

    orderbook_daemon.run(
        'BTCUSDT',
        api_key=api_key,
        secret_key=api_secret,
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main('PyCharm')
