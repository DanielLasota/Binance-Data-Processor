from dotenv import load_dotenv

from binance_archiver.data_quality_checker import conduct_data_quality_analysis_on_whole_directory, \
    conduct_data_quality_analysis_on_specified_csv_list
from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters
from binance_archiver.scraper import download_csv_data


if __name__ == '__main__':
    load_dotenv('binance-archiver-1.env')

    download_csv_data(
        date_range=['11-03-2025', '12-03-2025'],
        storage_connection_parameters=StorageConnectionParameters(),
        pairs=['BTCUSDT'],
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
        amount_of_files_to_be_downloaded_at_once=100
    )

    conduct_data_quality_analysis_on_specified_csv_list(
        csv_paths=[
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_spot_btcusdt_10-03-2025.csv',
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_usd_m_futures_btcusdt_10-03-2025.csv',
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_coin_m_futures_btcusd_perp_10-03-2025.csv',
        ]
    )

    conduct_data_quality_analysis_on_whole_directory('C:/Users/daniel/Documents/binance_archival_data/')
