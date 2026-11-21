import os
from dotenv import load_dotenv

from binance_data_processor import download_csv_data
from binance_data_processor import conduct_data_quality_analysis_on_specified_csv_list
from binance_data_processor import conduct_data_quality_analysis_on_whole_directory
from binance_data_processor.scraper.scraper_config import ScraperCfg

env_path = os.path.join(os.path.expanduser('~'), 'Documents/env/binance1-test.env')
load_dotenv(env_path)


if __name__ == '__main__':

    cfg = ScraperCfg(
        date_range=['31-10-2026', '31-10-2026'],
        pairs=[
            "BTCUSDT",
            "SOLUSDT",
            "XRPUSDT",
        ],
        markets=[
            'SPOT',
            'USD_M_FUTURES',
            'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            'DEPTH_SNAPSHOT'
        ],
        skip_existing=True,
        dump_path=f'D:/binance_archival_data/',
    )

    download_csv_data(cfg)

    # conduct_data_quality_analysis_on_specified_csv_list(
    #     csv_paths=[
    #         'C:/Users/daniel/Documents/binance_archival_data/binance_trade_stream_spot_trxusdt_02-04-2025.csv'
    #     ]
    # )

    # conduct_data_quality_analysis_on_whole_directory('C:/Users/daniel/Documents/binance_archival_data/')
