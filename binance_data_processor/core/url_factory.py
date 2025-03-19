from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


class URLFactory:
    __slots__ = ()

    @staticmethod
    def get_difference_depth_snapshot_url(
            asset_parameters: AssetParameters,
            limit: int | None = None
    ) -> str:

        base_urls = {
            Market.SPOT: 'https://api.binance.com/api/v3/depth?symbol={}&limit={}',
            Market.USD_M_FUTURES: 'https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={}',
            Market.COIN_M_FUTURES: 'https://dapi.binance.com/dapi/v1/depth?symbol={}&limit={}'
        }

        limits = {
            Market.SPOT: 5000,
            Market.USD_M_FUTURES: 1000,
            Market.COIN_M_FUTURES: 1000
        }

        market = asset_parameters.market

        base_url = base_urls.get(market)
        if base_url:
            actual_limit = limit if limit is not None else limits.get(market)
            if limit is not None and limit > limits.get(market, 0):
                print(f"Warning: Limit {limit} exceeds maximum allowed limit {limits[market]} for market {market}."
                      f"This may cause an 400 response")
            return base_url.format(asset_parameters.pairs[0], actual_limit)

        raise Exception(f'could not return depth snapshot url for: {asset_parameters}')

    @staticmethod
    def get_stream_url(asset_parameters: AssetParameters) -> str:

        base_urls = {
            Market.SPOT: 'wss://stream.binance.com:443/stream?streams={}&timeUnit=microsecond',
            Market.USD_M_FUTURES: 'wss://fstream.binance.com/stream?streams={}',
            Market.COIN_M_FUTURES: 'wss://dstream.binance.com/stream?streams={}'
        }

        stream_suffix_dict = {
            StreamType.TRADE_STREAM: '@trade',
            StreamType.DIFFERENCE_DEPTH_STREAM: '@depth@100ms'
        }

        stream_suffix = stream_suffix_dict.get(asset_parameters.stream_type)

        streams = '/'.join([f'{pair.lower()}{stream_suffix}' for pair in asset_parameters.pairs])
        base_url = base_urls.get(asset_parameters.market)
        if base_url:
            return base_url.format(streams)

        raise Exception(f'could not return url link for: {asset_parameters}')
