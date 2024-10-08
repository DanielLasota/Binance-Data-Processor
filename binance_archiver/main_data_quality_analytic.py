import os

from binance_archiver import conduct_csv_files_data_quality_analysis

if __name__ == '__main__':

    csvs_nest = os.path.join(os.path.expanduser("~").replace('\\', '/'), 'binance_archival_data')

    csv_paths = [
        f'{csvs_nest}/binance_trade_spot_suiusdt_05-10-2024.csv',
        f'{csvs_nest}/binance_difference_depth_spot_suiusdt_05-10-2024.csv'
    ]

    conduct_csv_files_data_quality_analysis(csv_paths=csv_paths)