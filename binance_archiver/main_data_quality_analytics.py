import os

from binance_archiver.data_quality_checker import conduct_data_quality_analysis_on_whole_directory, \
    conduct_data_quality_analysis_on_specified_csv_list

if __name__ == '__main__':

    csv_nest_directory = os.path.join(
        os.path.expanduser("~"),
        "Documents",
        "binance_archival_data"
    ).replace('\\', '/')

    csv_paths = [
        f'{csv_nest_directory}/binance_trade_stream_usd_m_futures_trxusdt_15-01-2025.csv',
        f'{csv_nest_directory}/binance_trade_stream_spot_trxusdt_15-01-2025.csv'
    ]

    conduct_data_quality_analysis_on_specified_csv_list(csv_paths=csv_paths)
    conduct_data_quality_analysis_on_whole_directory(csv_nest_directory=csv_nest_directory)
