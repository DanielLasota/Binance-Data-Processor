import os
from dotenv import load_dotenv

from binance_data_processor import download_csv_data
from binance_data_processor import StorageConnectionParameters
from binance_data_processor import conduct_data_quality_analysis_on_specified_csv_list
from binance_data_processor import conduct_data_quality_analysis_on_whole_directory

env_path = os.path.join(os.path.expanduser('~'), 'Documents/binance-archiver-23.env')
load_dotenv(env_path)


if __name__ == '__main__':

    download_csv_data(
        date_range=['01-04-2025', '01-04-2025'],
        storage_connection_parameters=StorageConnectionParameters(),
        pairs=['TRXUSDT'],
        markets=[
            'SPOT',
            'USD_M_FUTURES',
            'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            'DEPTH_SNAPSHOT',
        ],
        skip_existing=False,
        amount_of_files_to_be_downloaded_at_once=20
    )

    conduct_data_quality_analysis_on_specified_csv_list(
        csv_paths=[
            'C:/Users/daniel/Documents/binance_archival_data/binance_difference_depth_stream_usd_m_futures_trxusdt_14-03-2025.csv',
            'C:/Users/daniel/Documents/binance_archival_data/binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv',
        ]
    )

    conduct_data_quality_analysis_on_whole_directory('C:/Users/daniel/Documents/binance_archival_data/')
