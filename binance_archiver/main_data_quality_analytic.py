import os

from binance_archiver import conduct_csv_files_data_quality_analysis

if __name__ == '__main__':

    csvs_nest = os.path.join(os.path.expanduser("~"), 'binance_archival_data').replace('\\', '/')

    csv_paths = [
        f'{csvs_nest}/binance_trade_stream_spot_ethusdt_16-11-2024.csv',
        f'{csvs_nest}/binance_difference_depth_stream_spot_ethusdt_16-11-2024.csv'
    ]

    conduct_csv_files_data_quality_analysis(csv_paths=csv_paths)