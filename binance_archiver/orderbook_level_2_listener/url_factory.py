from typing import Optional
from binance_archiver.orderbook_level_2_listener.market_enum import Market


class URLFactory:
    """
    A utility class for generating URLs for various Binance API endpoints.

    :param market: The market type, which should be a value from the Market enum (SPOT, USD_M_FUTURES, COIN_M_FUTURES).
    :param pair: The trading pair as a string (e.g., 'BTCUSDT').
    :param limit: The maximum number of buy/sell levels to be returned in the snapshot.
    :return: A string containing the fully formed URL or None if the market type is not supported.

    Usage:
        The class methods are intended to be used statically. Here is how you can generate a URL for fetching
        orderbook snapshot data for the BTCUSDT pair on the spot market with a depth limit:

            URLFactory.get_snapshot_url(Market.SPOT, 'BTCUSDT', 100)

    """
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
    def get_transaction_stream_url(
            market: Market,
            pair: str
    ) -> Optional[str]:
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@trade',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@trade',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@trade'
        }.get(market, None)

    @staticmethod
    def get_orderbook_stream_url(
            market: Market,
            pair: str
    ) -> Optional[str]:
        return {
            Market.SPOT: f'wss://stream.binance.com:9443/ws/{pair.lower()}@depth@100ms',
            Market.USD_M_FUTURES: f'wss://fstream.binance.com/stream?streams={pair.lower()}@depth@100ms',
            Market.COIN_M_FUTURES: f'wss://dstream.binance.com/stream?streams={pair.lower()}@depth@100ms'
        }.get(market, None)
