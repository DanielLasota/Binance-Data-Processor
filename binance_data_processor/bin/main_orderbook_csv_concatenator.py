import os
from dotenv import load_dotenv

from binance_data_processor import make_merged_csvs
from binance_data_processor.scraper.concatenator_config import DataConcatenatorConfig

env_path = os.path.join(os.path.expanduser('~'), 'Documents/env/binance-archiver-ba2-v2-prod.env')
load_dotenv(env_path)


if __name__ == '__main__':

    data_concatenator_config = DataConcatenatorConfig(
        date_range=['04-11-2025', '04-11-2025'],
        pairs=[
            "BTCUSDT",
            # "XRPUSDT",
            # "SOLUSDT",
        ],
        markets=[
            'SPOT',
            'USD_M_FUTURES',
            # 'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            'DEPTH_SNAPSHOT'
        ],
        should_join_pairs_into_one_csv=False,
        should_join_markets_into_one_csv=False,
        csvs_nest_catalog='D:/binance_archival_data/',
        dump_path='D:/merged_csvs/',
        skip_existing=True
    )
    make_merged_csvs(data_concatenator_config)

