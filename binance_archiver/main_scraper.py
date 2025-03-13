from dotenv import load_dotenv

from binance_archiver.data_quality_checker import conduct_csv_files_data_quality_analysis
from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters
from binance_archiver.scraper import download_csv_data


if __name__ == '__main__':
    load_dotenv('binance-archiver-1.env')

    storage_connection_parameters = StorageConnectionParameters()

    download_csv_data(
        date_range=['08-03-2025', '08-03-2025'],
        storage_connection_parameters=storage_connection_parameters,
        pairs=['TRXUSDT'],
        markets=['USD_M_FUTURES', 'COIN_M_FUTURES'],
        stream_types=['TRADE_STREAM', 'DIFFERENCE_DEPTH_STREAM']
    )

    conduct_csv_files_data_quality_analysis(
        csv_paths=[
            'C:/Users/daniel/Documents/binance_archival_data/binance_trade_stream_usd_m_futures_trxusdt_08-03-2025.csv',
        ]
    )
