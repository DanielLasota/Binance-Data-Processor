from binance_archiver.data_sink_config import DataSinkConfig
from binance_archiver.listener_facade import launch_data_listener
from binance_archiver.data_sink_facade import launch_data_sink, BinanceDataSink
from binance_archiver.load_config import load_config_from_json

from binance_archiver.scraper import (
    download_csv_data,
    conduct_csv_files_data_quality_analysis,
    conduct_whole_directory_of_csvs_data_quality_analysis
)



__all__ = [
    'launch_data_sink',
    'launch_data_listener',
    'download_csv_data',
    'conduct_whole_directory_of_csvs_data_quality_analysis',
    'conduct_csv_files_data_quality_analysis',
    'load_config_from_json',
    'BinanceDataSink',
    'DataSinkConfig'
]

__version__ = "0.0.1"
__author__ = "Daniel Lasota <grossmann.root@gmail.com>"
__description__ = "launch data sink or listening engine"
__email__ = "grossmann.root@gmail.com"
__url__ = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
__status__ = "development"
__date__ = "25-09-2024"
