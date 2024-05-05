from typing import Optional

from orderbook_level_2_listener.market_enum import Market


class URLFactory:
    BASE_URLS = {
        Market.SPOT: "wss://stream.binance.com:9443",
        Market.USD_M_FUTURES: "wss://fstream.binance.com",
        Market.COIN_M_FUTURES: "wss://dstream.binance.com"
    }

    @staticmethod
    def get_snapshot_url(
            market: Market,
            pair: str,
            limit: int
    ) -> Optional[str]:
        return {
            Market.SPOT: f'https://api.binance.com/api/v3/depth?symbol={pair}&limit={limit}',
            Market.USD_M_FUTURES: f'https://fapi.binance.com/fapi/v1/depth?symbol={pair}&limit={limit}',
            Market.COIN_M_FUTURES: f'https://dapi.binance.com/dapi/v1/depth?symbol={pair}&limit={limit}'
        }.get(market, None)

    @staticmethod
    def get_transaction_stream_url(market, pair):
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@trade',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@trade',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@trade'
        }.get(market, None)

    @staticmethod
    def get_orderbook_stream_url(market, pair):
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@depth@100ms',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@depth@100ms',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@depth'
        }.get(market, None)