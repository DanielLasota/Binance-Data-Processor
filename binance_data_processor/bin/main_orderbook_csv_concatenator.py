import os
from dotenv import load_dotenv

from binance_data_processor import make_merged_csvs
from binance_data_processor.scraper.concatenator_config import DataConcatenatorConfig

env_path = os.path.join(os.path.expanduser('~'), 'Documents/env/binance-archiver-ba2-v2-test.env')
load_dotenv(env_path)


if __name__ == '__main__':

    cfg = DataConcatenatorConfig(
        date_range=['26-01-2026', '26-01-2026'],
        pairs=[
            "BTCUSDT",
            # "XRPUSDT",
            # "SOLUSDT",
        ],
        markets=[
            'SPOT',
            # 'USD_M_FUTURES',
            # 'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            # 'DIFFERENCE_DEPTH_STREAM',
            # 'DEPTH_SNAPSHOT'
        ],
        join_pairs_into_one_csv=False,
        join_markets_into_one_csv=False,
        csvs_nest_catalog='D:/binance_archival_data/',
        dump_path='D:/merged_csvs/',
        skip_existing=True
    )
    make_merged_csvs(cfg)
