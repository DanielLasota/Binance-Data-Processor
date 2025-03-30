from binance_data_processor.enums.data_sink_config import DataSinkConfig
from binance_data_processor.enums.data_sink_config import StorageConnectionParameters
from binance_data_processor.core.load_config import load_config_from_json
from binance_data_processor.data_sink.data_sink_facade import launch_data_sink
from binance_data_processor.data_sink.data_sink_facade import BinanceDataSink
from binance_data_processor.listener.listener_facade import launch_data_listener
from binance_data_processor.listener.listener_facade import BinanceDataListener
from binance_data_processor.scraper.scraper import download_csv_data
from binance_data_processor.scraper.data_quality_checker import conduct_data_quality_analysis_on_specified_csv_list
from binance_data_processor.scraper.data_quality_checker import conduct_data_quality_analysis_on_whole_directory


__all__ = [
    'DataSinkConfig',
    'StorageConnectionParameters',
    'load_config_from_json',
    'launch_data_sink',
    'BinanceDataSink',
    'launch_data_listener',
    'BinanceDataListener',
    'download_csv_data',
    'conduct_data_quality_analysis_on_specified_csv_list',
    'conduct_data_quality_analysis_on_whole_directory'
]

__version__ = "0.0.1"
__author__ = "Daniel Lasota <grossmann.root@gmail.com>"
__description__ = "launch data sink or listening engine. Then Run scraper with data quality certificate"
__email__ = "grossmann.root@gmail.com"
__url__ = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
__status__ = "development"
__date__ = "25-09-2024"
