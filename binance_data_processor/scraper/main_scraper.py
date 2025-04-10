import os
from dotenv import load_dotenv

from binance_data_processor import download_csv_data
from binance_data_processor import StorageConnectionParameters
from binance_data_processor import conduct_data_quality_analysis_on_specified_csv_list
from binance_data_processor import conduct_data_quality_analysis_on_whole_directory

env_path = os.path.join(os.path.expanduser('~'), 'Documents/binance-archiver-ba2-v2-prod.env')
load_dotenv(env_path)


if __name__ == '__main__':

    download_csv_data(
        storage_connection_parameters=StorageConnectionParameters(),
        date_range=['09-04-2025', '09-04-2025'],
        pairs=[
            # 'ADAUSDT',
            # "SOLUSDT",
            # "XRPUSDT",
            # "DOGEUSDT",
            # "ADAUSDT",
            # "SHIBUSDT",
            # "LTCUSDT",
            # "AVAXUSDT",
            "TRXUSDT",
            # "DOTUSDT",
            # "BCHUSDT",
            # "SUIUSDT"
        ],
        markets=[
            # 'SPOT',
            'USD_M_FUTURES',
            # 'COIN_M_FUTURES'
        ],
        stream_types=[
            # 'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            # 'DEPTH_SNAPSHOT',
        ],
        skip_existing=False,
        amount_of_files_to_be_downloaded_at_once=20
    )

    conduct_data_quality_analysis_on_specified_csv_list(
        csv_paths=[
            'C:/Users/daniel/Documents/binance_archival_data/binance_trade_stream_spot_trxusdt_02-04-2025.csv'
        ]
    )

    conduct_data_quality_analysis_on_whole_directory('C:/Users/daniel/Documents/binance_archival_data/')
