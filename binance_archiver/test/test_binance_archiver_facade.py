import re
import threading
import time
from datetime import datetime, timezone
from queue import Queue
from unittest.mock import patch, MagicMock
import pytest

from ..abstract_base_classes import Observer
from binance_archiver.enum_.asset_parameters import AssetParameters
from ..exceptions import (
    ClassInstancesAmountLimitException,
)

from ..listener_facade import (
    launch_data_listener,
    BinanceDataListener
)
from ..data_sink_facade import BinanceDataSink
from .. import launch_data_sink, DataSinkConfig
from ..snapshot_manager import DepthSnapshotStrategy, DataSinkDepthSnapshotStrategy, ListenerDepthSnapshotStrategy, \
    DepthSnapshotService
from ..listener_observer_updater import ListenerObserverUpdater
from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters
from ..stream_data_save_and_sender import StreamDataSaverAndSender
from ..queue_pool import ListenerQueuePool, DataSinkQueuePool
from ..stream_service import StreamService
from ..commandline_interface import CommandLineInterface
from ..timestamps_generator import TimestampsGenerator
from ..fastapi_manager import FastAPIManager

from ..setup_logger import setup_logger
from ..difference_depth_queue import DifferenceDepthQueue
from ..stream_id import StreamId
from ..trade_queue import TradeQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType


class TestArchiverFacade:

    def test_init(self):
        assert True

    class TestDataSinkFacade:

        def test_given_archiver_facade_when_init_then_global_shutdown_flag_is_false(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            archiver_facade = BinanceDataSink(data_sink_config=data_sink_config)

            assert not archiver_facade.global_shutdown_flag.is_set()

            del archiver_facade

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_archiver_facade_when_init_then_queues_are_set_properly(self):

            queue_pool = DataSinkQueuePool()

            spot_diff_queue = queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(spot_diff_queue,
                              DifferenceDepthQueue), "SPOT difference_depth_stream powinien być DifferenceDepthQueue"

            spot_trade_queue = queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM)
            assert isinstance(spot_trade_queue, TradeQueue), "SPOT trade_stream powinien być TradeQueue"

            usdm_diff_queue = queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(usdm_diff_queue,
                              DifferenceDepthQueue), "USD-M difference_depth_stream powinien być DifferenceDepthQueue"

            usdm_trade_queue = queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM)
            assert isinstance(usdm_trade_queue, TradeQueue), "USD-M trade_stream powinien być TradeQueue"

            coinm_diff_queue = queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(coinm_diff_queue,
                              DifferenceDepthQueue), "COIN-M difference_depth_stream powinien być DifferenceDepthQueue"

            coinm_trade_queue = queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            assert isinstance(coinm_trade_queue, TradeQueue), "COIN-M trade_stream powinien być TradeQueue"

            assert len(DifferenceDepthQueue._instances) == 3, "Powinno być 3 instancje DifferenceDepthQueue"
            assert len(TradeQueue._instances) == 3, "Powinno być 3 instancje TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_run_call_when_threads_invoked_then_correct_threads_are_started(self):

            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            data_sink_facade = launch_data_sink(data_sink_config=data_sink_config)

            time.sleep(3)

            num_markets = len(config_from_json["instruments"])

            expected_stream_service_threads = num_markets * 2
            expected_stream_writer_threads = num_markets * 2
            expected_snapshot_daemon_threads = num_markets

            total_expected_threads = (expected_stream_service_threads + expected_stream_writer_threads
                                      + expected_snapshot_daemon_threads)

            active_threads = threading.enumerate()
            daemon_threads = [thread for thread in active_threads if 'stream_service' in thread.name or
                              'stream_writer' in thread.name or 'snapshot_daemon'
                              in thread.name]

            thread_names = [thread.name for thread in daemon_threads]

            for market in ["SPOT", "USD_M_FUTURES", "COIN_M_FUTURES"]:
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names

            assert len(daemon_threads) == total_expected_threads

            data_sink_facade.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_archiver_facade_initialization_in_data_sink_mode(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }
            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )
            data_sink_facade = BinanceDataSink(data_sink_config=data_sink_config)

            assert isinstance(data_sink_facade.queue_pool, DataSinkQueuePool)
            assert isinstance(data_sink_facade.stream_service, StreamService)
            assert isinstance(data_sink_facade.command_line_interface, CommandLineInterface)
            assert isinstance(data_sink_facade.fast_api_manager, FastAPIManager)
            assert isinstance(data_sink_facade.stream_data_saver_and_sender, StreamDataSaverAndSender)
            assert isinstance(data_sink_facade.depth_snapshot_service, DepthSnapshotService)

            data_sink_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        @pytest.mark.skip
        def test_given_archiver_facade_when_shutdown_called_then_no_threads_are_left(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            data_sink = launch_data_sink(data_sink_config=data_sink_config)

            time.sleep(15)

            data_sink.shutdown()

            active_threads = []

            for _ in range(20):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(
                active_threads) == 0, f"Still active threads after shutdown: {[thread.name for thread in active_threads]}"

            del data_sink

        @pytest.mark.parametrize('execution_number', range(1))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(
                self, execution_number):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )
            data_sink = BinanceDataSink(data_sink_config=data_sink_config)
            time.sleep(5)

            data_sink.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            for _ in active_threads: print(_)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del data_sink_config

    class TestListenerFacade:

        def test_given_archiver_facade_when_init_then_global_shutdown_flag_is_false(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            listener_facade = BinanceDataListener(data_sink_config=data_sink_config)

            assert not listener_facade.global_shutdown_flag.is_set()

            del listener_facade

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_archiver_facade_when_init_then_queues_are_set_properly(self):
            queue_pool = ListenerQueuePool()

            spot_diff_queue = queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(spot_diff_queue,
                              DifferenceDepthQueue), "SPOT difference_depth_stream powinien być DifferenceDepthQueue"

            spot_trade_queue = queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM)
            assert isinstance(spot_trade_queue, TradeQueue), "SPOT trade_stream powinien być TradeQueue"

            usdm_diff_queue = queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(usdm_diff_queue,
                              DifferenceDepthQueue), "USD-M difference_depth_stream powinien być DifferenceDepthQueue"

            usdm_trade_queue = queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM)
            assert isinstance(usdm_trade_queue, TradeQueue), "USD-M trade_stream powinien być TradeQueue"

            coinm_diff_queue = queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM)
            assert isinstance(coinm_diff_queue,
                              DifferenceDepthQueue), "COIN-M difference_depth_stream powinien być DifferenceDepthQueue"

            coinm_trade_queue = queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            assert isinstance(coinm_trade_queue, TradeQueue), "COIN-M trade_stream powinien być TradeQueue"

            assert len(DifferenceDepthQueue._instances) == 3, "Powinno być 3 instancje DifferenceDepthQueue"
            assert len(TradeQueue._instances) == 3, "Powinno być 3 instancje TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_run_call_when_threads_invoked_then_correct_threads_are_started(self):

            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            data_listener = launch_data_listener(data_sink_config=data_sink_config)

            time.sleep(3)

            num_markets = len(config_from_json["instruments"])

            expected_stream_service_threads = num_markets * 2
            expected_snapshot_daemon_threads = num_markets

            total_expected_threads = (
                    expected_stream_service_threads
                    + expected_snapshot_daemon_threads
            )

            active_threads = threading.enumerate()
            daemon_threads = [thread for thread in active_threads if 'stream_service' in thread.name or
                              'stream_writer' in thread.name or 'snapshot_daemon'
                              in thread.name]

            thread_names = [thread.name for thread in daemon_threads]

            for market in ["SPOT", "USD_M_FUTURES", "COIN_M_FUTURES"]:
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH_STREAM}' in thread_names
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE_STREAM}' in thread_names
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names

            assert len(daemon_threads) == total_expected_threads

            data_listener.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_archiver_facade_initialization_in_listener_mode(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }
            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            observers = [MagicMock(spec=Observer)]

            listener_facade = BinanceDataListener(
                data_sink_config=data_sink_config,
                init_observers=observers
            )

            assert listener_facade._observers == observers
            assert isinstance(listener_facade.listener_observer_updater, ListenerObserverUpdater)
            assert listener_facade.listener_observer_updater.observers == observers

            listener_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_attach_and_detach_observers(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }
            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )
            observer1 = MagicMock(spec=Observer)
            observer2 = MagicMock(spec=Observer)

            listener_facade = BinanceDataListener(
                data_sink_config=data_sink_config,
                init_observers=[observer1]
            )

            listener_facade.attach(observer2)
            assert listener_facade._observers == [observer1, observer2], "Observer2 should be attached"

            listener_facade.detach(observer1)
            assert listener_facade._observers == [observer2], "Observer1 should be detached"

            listener_facade.shutdown()
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        @pytest.mark.skip
        def test_given_archiver_facade_when_shutdown_called_then_no_threads_are_left(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_facade = launch_data_sink(config)

            time.sleep(15)

            archiver_facade.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(
                active_threads) == 0, f"Still active threads after shutdown: {[thread.name for thread in active_threads]}"

            del archiver_facade

        @pytest.mark.parametrize('execution_number', range(1))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(
                self, execution_number):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            archiver_daemon = launch_data_listener(data_sink_config=data_sink_config)

            time.sleep(5)

            archiver_daemon.shutdown()

            active_threads = []

            for _ in range(40):
                active_threads = [
                    thread for thread in threading.enumerate()
                    if thread is not threading.current_thread()
                ]
                if not active_threads:
                    break
                time.sleep(1)

            for _ in active_threads: print(_)

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del archiver_daemon

    class TestObserverPattern:

        class MockObserver(Observer):
            def __init__(self):
                self.messages = []

            def update(self, message):
                self.messages.append(message)

        def test_observer_receives_notifications(self):
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )
            observer = self.MockObserver()
            archiver_facade = BinanceDataListener(
                data_sink_config=data_sink_config,
                init_observers=[observer]
            )

            test_message = "Test Message"
            archiver_facade.queue_pool.global_queue.put(test_message)

            whistleblower_thread = threading.Thread(
                target=archiver_facade.listener_observer_updater.process_global_queue)
            whistleblower_thread.start()

            time.sleep(1)

            archiver_facade.shutdown()
            whistleblower_thread.join()

            assert observer.messages == [test_message], "Observer should have received the test message"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestWhistleblower:

        def test_whistleblower_processes_messages_and_notifies_observers(self):
            observers = [MagicMock(spec=Observer)]
            global_queue = Queue()
            global_shutdown_flag = threading.Event()

            whistleblower = ListenerObserverUpdater(
                observers=observers,
                global_queue=global_queue,
                global_shutdown_flag=global_shutdown_flag
            )

            test_message = "Test Message"
            global_queue.put(test_message)

            thread = threading.Thread(target=whistleblower.process_global_queue)
            thread.start()

            time.sleep(1)

            global_shutdown_flag.set()
            thread.join()

            for observer in observers:
                observer.update.assert_called_with(test_message)

        def test_whistleblower_exits_on_shutdown(self):
            observers = [MagicMock(spec=Observer)]
            global_queue = Queue()
            global_shutdown_flag = threading.Event()
            whistleblower = ListenerObserverUpdater(
                observers=observers,
                global_queue=global_queue,
                global_shutdown_flag=global_shutdown_flag
            )

            thread = threading.Thread(target=whistleblower.process_global_queue)
            thread.start()

            global_shutdown_flag.set()
            thread.join()

            assert not thread.is_alive(), "Whistleblower thread should have exited"

    class TestQueuePoolDataSink:

        def test_given_queue_pool_when_initialized_then_queues_are_set_properly(self):
            queue_pool = DataSinkQueuePool()

            expected_keys = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.SPOT, StreamType.TRADE_STREAM),
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM),
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            }

            assert set(queue_pool.queue_lookup.keys()) == expected_keys, "queue_lookup keys do not match expected keys"

            assert isinstance(queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)
            assert isinstance(queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)
            assert isinstance(queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)

            assert isinstance(queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM), TradeQueue)
            assert isinstance(queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM), TradeQueue)
            assert isinstance(queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM), TradeQueue)

            assert len(TradeQueue._instances) == 3, "There should be 3 instances of TradeQueue"
            assert len(DifferenceDepthQueue._instances) == 3, "There should be 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_get_queue_called_then_returns_correct_queue(self):
            queue_pool = DataSinkQueuePool()

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE_STREAM): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                queue = queue_pool.get_queue(market, stream_type)
                assert isinstance(queue, expected_queue_type), (
                    f"Queue for {market}, {stream_type} should be {expected_queue_type}")
                assert queue.market == market, f"Queue market should be {market}"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_trade_queues_created_then_exception_is_thrown(self):
            queue_pool = DataSinkQueuePool()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_trade_queue = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_difference_depth_queues_created_then_exception_is_thrown(self):
            queue_pool = DataSinkQueuePool()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_queue_pool_initialization_in_listener_mode(self):
            queue_pool = ListenerQueuePool()

            assert queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"

            assert queue_pool.get_queue(Market.SPOT,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.USD_M_FUTURES,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.COIN_M_FUTURES,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM).queue == queue_pool.global_queue

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestQueuePoolListener:

        def test_given_queue_pool_when_initialized_then_queues_are_set_properly(self):
            queue_pool = ListenerQueuePool()

            expected_keys = [
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.SPOT, StreamType.TRADE_STREAM),
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM),
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM)
            ]

            assert set(queue_pool.queue_lookup.keys()) == set(
                expected_keys), "queue_lookup keys do not match expected keys"

            assert isinstance(queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)
            assert isinstance(queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)
            assert isinstance(queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM),
                              DifferenceDepthQueue)

            assert isinstance(queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM), TradeQueue)
            assert isinstance(queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM), TradeQueue)
            assert isinstance(queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM), TradeQueue)

            assert len(TradeQueue._instances) == 3, "There should be 3 instances of TradeQueue"
            assert len(DifferenceDepthQueue._instances) == 3, "There should be 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_get_queue_called_then_returns_correct_queue(self):
            queue_pool = ListenerQueuePool()

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE_STREAM): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH_STREAM): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE_STREAM): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                queue = queue_pool.get_queue(market, stream_type)
                assert isinstance(queue, expected_queue_type), (
                    f"Queue for {market}, {stream_type} should be an instance of {expected_queue_type}"
                )
                assert queue.market == market, f"Queue market should be {market}"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_trade_queues_created_then_exception_is_thrown(self):
            queue_pool = ListenerQueuePool()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_trade_queue = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_difference_depth_queues_created_then_exception_is_thrown(self):
            queue_pool = ListenerQueuePool()

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fourth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 3 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_queue_pool_initialization_in_listener_mode(self):
            queue_pool = ListenerQueuePool()

            assert queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"

            assert queue_pool.get_queue(Market.SPOT,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.SPOT, StreamType.TRADE_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.USD_M_FUTURES,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.USD_M_FUTURES, StreamType.TRADE_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.COIN_M_FUTURES,
                                        StreamType.DIFFERENCE_DEPTH_STREAM).queue == queue_pool.global_queue
            assert queue_pool.get_queue(Market.COIN_M_FUTURES, StreamType.TRADE_STREAM).queue == queue_pool.global_queue

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

    class TestStreamService:

        def test_stream_service_initialization_with_global_queue(self):
            setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = ListenerQueuePool()

            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

            stream_service = StreamService(
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag,
                data_sink_config=data_sink_config
            )

            assert stream_service.queue_pool.global_queue is not None, "Global queue should be initialized in LISTENER mode"

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_stream_service_runs_streams_in_listener_mode(self):
            setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = ListenerQueuePool()
            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )
            stream_service = StreamService(
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag,
                data_sink_config=data_sink_config
            )

            with patch.object(StreamService, 'start_stream_service') as mock_start_stream_service:
                stream_service.run()
                assert mock_start_stream_service.call_count == 6, "Should start two stream services in LISTENER mode"

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

    class TestSnapshotManager:

        def test_init(self):
            assert True

        class TestSnapshotManager:

            def test_given_listener_strategy_when_snapshot_received_then_snapshot_put_into_global_queue(self):
                json_config = {
                    'instruments': {
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds': 60
                }
                data_sink_config = DataSinkConfig(
                    instruments={
                        "spot": json_config['instruments']['spot']
                    }
                )
                setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()

                depth_snapshot_service = DepthSnapshotService(
                    snapshot_strategy=ListenerDepthSnapshotStrategy(global_queue=global_queue),
                    global_shutdown_flag=global_shutdown_flag,
                    data_sink_config=data_sink_config
                )

                with patch.object(
                        DepthSnapshotService,
                        '_request_snapshot_with_timestamps',
                        return_value='{"sample_data": "sample_data","_rq":1234567890,"_rc":1234567891}'
                ):
                    asset_parameters = AssetParameters(
                        market=Market.SPOT,
                        stream_type=StreamType.DEPTH_SNAPSHOT,
                        pairs=['BTCUSDT']
                    )
                    daemon_thread = threading.Thread(
                        target=depth_snapshot_service._snapshot_daemon,
                        args=[asset_parameters],
                        name='snapshot_daemon_thread'
                    )
                    daemon_thread.start()

                    time.sleep(2)

                    global_shutdown_flag.set()

                    daemon_thread.join(timeout=2)

                    assert not global_queue.empty(), "Global queue powinna zawierać dane snapshotu"

                    message = global_queue.get()
                    assert isinstance(message, str), "Should have been serialized as string JSON"

                    expected_snapshot = '{"sample_data": "sample_data","_rq":1234567890,"_rc":1234567891}'

                    assert message == expected_snapshot, "Dane snapshotu powinny zgadzać się z oczekiwanymi"

            def test_given_data_sink_strategy_when_snapshot_received_then_data_saver_methods_called(self):
                config = {
                    'instruments': {
                        "spot": ["BTCUSDT"]
                    },
                    'websocket_lifetime_seconds': 60
                }
                setup_logger()
                global_shutdown_flag = threading.Event()

                data_sink_config = DataSinkConfig(
                    instruments={
                        "spot": config['instruments']["spot"]
                    }
                )

                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkDepthSnapshotStrategy(
                    data_saver=data_saver
                )

                snapshot_manager = DepthSnapshotService(
                    snapshot_strategy=snapshot_strategy,
                    data_sink_config=data_sink_config,
                    global_shutdown_flag=global_shutdown_flag
                )

                asset_parameters = AssetParameters(
                    market=Market.SPOT,
                    stream_type=StreamType.DEPTH_SNAPSHOT,
                    pairs=["BTCUSDT"]
                )
                # asset_parameters.get_asset_parameter_with_specified_pair = lambda pair: asset_parameters

                with patch.object(
                        DepthSnapshotService,
                        '_request_snapshot_with_timestamps',
                        return_value='{"sample_data": "sample_data","_rq":1234567890,"_rc":1234567891}'
                ):
                    with patch.object(StreamDataSaverAndSender, 'get_file_name', return_value='file_name.json'):
                        daemon_thread = threading.Thread(
                            target=snapshot_manager._snapshot_daemon,
                            args=[asset_parameters],
                            name='snapshot_daemon_thread'
                        )
                        daemon_thread.start()

                        time.sleep(0.5)

                        global_shutdown_flag.set()
                        daemon_thread.join(timeout=2)

                        expected_snapshot = '{"sample_data": "sample_data","_rq":1234567890,"_rc":1234567891}'

                        data_saver.save_data.assert_called_with(
                            json_content=expected_snapshot,
                            file_save_catalog="dump/",
                            file_name="file_name.json"
                        )

            def test_given_exception_in_get_snapshot_when_snapshot_fetched_then_error_logged_and_no_snapshot_processed(self):
                data_sink_config = DataSinkConfig(instruments={"spot": ["BTCUSDT"]})

                setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()
                snapshot_strategy = ListenerDepthSnapshotStrategy(global_queue=global_queue)

                snapshot_manager = DepthSnapshotService(
                    snapshot_strategy=snapshot_strategy,
                    data_sink_config=data_sink_config,
                    global_shutdown_flag=global_shutdown_flag
                )

                asset_parameters = AssetParameters(
                    market=Market.SPOT,
                    stream_type=StreamType.DEPTH_SNAPSHOT,
                    pairs=["BTCUSDT"]
                )

                with patch.object(
                        DepthSnapshotService,
                        '_request_snapshot_with_timestamps',
                        side_effect=Exception("Test exception")
                ):
                    with patch.object(snapshot_manager.logger, 'error') as mock_logger_error:
                        daemon_thread = threading.Thread(
                            target=snapshot_manager._snapshot_daemon,
                            args=[asset_parameters],
                            name='snapshot_daemon_thread'
                        )
                        daemon_thread.start()

                        # Dajemy czas wątkowi na wykonanie przynajmniej jednej iteracji
                        time.sleep(0.5)

                        global_shutdown_flag.set()

                        daemon_thread.join(timeout=2)

                        # Sprawdzamy, czy błąd został zalogowany
                        mock_logger_error.assert_called()
                        # Globalna kolejka nie powinna zawierać żadnych danych, gdy wystąpi wyjątek
                        assert global_queue.empty(), "Global queue powinna być pusta po wyjątku"

            def test_given_shutdown_flag_set_when_daemon_running_then_thread_exits(self):
                data_sink_config = DataSinkConfig(
                    instruments={
                        "spot": ["BTCUSDT"]
                    }
                )

                setup_logger()
                global_shutdown_flag = threading.Event()

                global_queue = Queue()
                snapshot_strategy = ListenerDepthSnapshotStrategy(global_queue=global_queue)

                snapshot_manager = DepthSnapshotService(
                    snapshot_strategy=snapshot_strategy,
                    data_sink_config=data_sink_config,
                    global_shutdown_flag=global_shutdown_flag
                )

                asset_parameters = AssetParameters(
                    market=Market.SPOT,
                    stream_type=StreamType.DEPTH_SNAPSHOT,
                    pairs=["BTCUSDT"]
                )

                with patch.object(DepthSnapshotService, '_request_snapshot_with_timestamps') as mock_get_snapshot:
                    daemon_thread = threading.Thread(
                        target=snapshot_manager._snapshot_daemon,
                        args=[asset_parameters],
                        name='snapshot_daemon_thread'
                    )
                    daemon_thread.start()

                    time.sleep(0.5)

                    global_shutdown_flag.set()

                    daemon_thread.join(timeout=2)

                    assert not daemon_thread.is_alive(), "snapshot_daemon thread should end if global_shutdown_flag.set"

            def test_given_successful_response_when_get_snapshot_called_then_data_and_timestamps_returned(self):
                data_sink_config = DataSinkConfig(
                    instruments={"spot": ["BTCUSDT"]}
                )

                setup_logger()
                global_shutdown_flag = threading.Event()

                snapshot_strategy = MagicMock(spec=DepthSnapshotStrategy)

                snapshot_manager = DepthSnapshotService(
                    snapshot_strategy=snapshot_strategy,
                    data_sink_config=data_sink_config,
                    global_shutdown_flag=global_shutdown_flag
                )

                mock_response_text = '{"bids":[],"asks":[]}'

                with patch('requests.get') as mock_get:
                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    mock_response.text = mock_response_text
                    mock_get.return_value = mock_response

                    with patch.object(
                            TimestampsGenerator,
                            'get_utc_timestamp_epoch_milliseconds',
                            side_effect=[1000, 2000]
                    ):
                        returned_entry = snapshot_manager._request_snapshot_with_timestamps(
                            asset_parameters=AssetParameters(
                                market=Market.SPOT,
                                stream_type=StreamType.DEPTH_SNAPSHOT,
                                pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
                            )
                        )

                        print(f'returned_entry {returned_entry}')
                        expected_snapshot = '{"bids":[],"asks":[],"_rq":1000,"_rc":2000}'
                        assert returned_entry == expected_snapshot, "Snapshot z timestampami powinien być zgodny z oczekiwanym wynikiem"

            def test_given_shutdown_flag_set_when_sleep_with_flag_check_called_then_method_exits_early(self):
                data_sink_config = DataSinkConfig(
                    instruments={"spot": ["BTCUSDT"]}
                )
                data_sink_config.file_save_catalog = ""

                logger = setup_logger()
                global_shutdown_flag = threading.Event()

                snapshot_manager = DepthSnapshotService(
                    snapshot_strategy=MagicMock(spec=DepthSnapshotStrategy),
                    data_sink_config=data_sink_config,
                    global_shutdown_flag=global_shutdown_flag
                )

                # W osobnym wątku ustawiamy flagę po 0.5 sekundy
                def set_flag():
                    time.sleep(0.5)
                    global_shutdown_flag.set()

                threading.Thread(target=set_flag).start()

                start_time = time.time()
                snapshot_manager._sleep_with_flag_check(duration=5)
                elapsed_time = time.time() - start_time

                assert elapsed_time < 1.5, (
                    "Metoda _sleep_with_flag_check powinna zakończyć działanie szybko po ustawieniu flagi global_shutdown_flag"
                )

        class TestListenerSnapshotStrategy:

            def test_given_snapshot_when_handle_snapshot_called_then_snapshot_put_into_global_queue(self):
                global_queue = Queue()
                snapshot_strategy = ListenerDepthSnapshotStrategy(global_queue=global_queue)

                json_content = '{"snapshot":"data","_rq":1234567890,"_rc":1234567891}'
                snapshot_strategy.handle_snapshot(
                    json_content=json_content,
                    file_save_catalog="",
                    file_name=""
                )

                assert not global_queue.empty(), "Global queue should contain snapshot data"
                message = global_queue.get()
                assert message == json_content, "Dane snapshotu powinny być zserializowane jako string JSON"

        class TestDataSinkSnapshotStrategy:

            def test_given_snapshot_when_handle_snapshot_called_then_save_data_called(self):
                data_saver = MagicMock(spec=StreamDataSaverAndSender)
                snapshot_strategy = DataSinkDepthSnapshotStrategy(data_saver=data_saver)

                json_content = '{"snapshot":"data","_rq":1234567890,"_rc":1234567891}'
                file_name = "file_name.json"
                file_save_catalog = "dump_path"

                snapshot_strategy.handle_snapshot(
                    json_content=json_content,
                    file_save_catalog=file_save_catalog,
                    file_name=file_name
                )

                data_saver.save_data.assert_called_once_with(
                    json_content=json_content,
                    file_save_catalog=file_save_catalog,
                    file_name=file_name
                )

    class TestCommandLineInterface:

        def test_given_modify_subscription_when_adding_asset_then_asset_is_added_to_instruments(self):
            config_from_json = {
                'instruments': {
                    'spot': ['BTCUSDT']
                },
                'websocket_life_time_seconds': 60,
                'snapshot_fetcher_interval_seconds': 60,
                'file_duration_seconds': 3600
            }

            data_sink_config = DataSinkConfig(
                instruments=config_from_json['instruments'],
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                }
            )

            global_shutdown_flag = threading.Event()
            queue_pool = DataSinkQueuePool()

            stream_service = StreamService(
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag,
                data_sink_config=data_sink_config
            )
            cli = CommandLineInterface(
                stream_service=stream_service,
                data_sink_config=data_sink_config
            )

            message = {'modify_subscription': {'type': 'subscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' in data_sink_config.instruments.get_pairs(market=Market.SPOT), "Asset not added to instruments"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_modify_subscription_when_removing_asset_then_asset_is_removed_from_instruments(self):
            config_from_json = {
                'instruments': {
                    'spot': ['BTCUSDT', 'BNBUSDT']
                },
                'websocket_life_time_seconds': 60,
                'snapshot_fetcher_interval_seconds': 60,
                'file_duration_seconds': 3600
            }

            data_sink_config = DataSinkConfig(
                instruments=config_from_json['instruments'],
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                }
            )

            global_shutdown_flag = threading.Event()
            queue_pool = DataSinkQueuePool()

            stream_service = StreamService(
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag,
                data_sink_config=data_sink_config
            )
            cli = CommandLineInterface(
                stream_service=stream_service,
                data_sink_config=data_sink_config
            )

            message = {'modify_subscription': {'type': 'unsubscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' not in data_sink_config.instruments.get_pairs(market=Market.SPOT), "Asset not added to instruments"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_handle_command_with_invalid_command_logs_warning(self):
            config_from_json = {
                'instruments': {
                    'spot': ['BTCUSDT', 'BNBUSDT']
                },
                'websocket_life_time_seconds': 60,
                'snapshot_fetcher_interval_seconds': 60,
                'file_duration_seconds': 3600
            }

            data_sink_config = DataSinkConfig(
                instruments=config_from_json['instruments'],
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                }
            )

            logger = setup_logger()

            global_shutdown_flag = threading.Event()
            queue_pool = DataSinkQueuePool()

            stream_service = StreamService(
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag,
                data_sink_config=data_sink_config
            )
            cli = CommandLineInterface(
                stream_service=stream_service,
                data_sink_config=data_sink_config
            )

            with patch.object(logger, 'warning') as mock_warning:
                message = {'invalid_command': {'type': 'subscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
                cli.handle_command(message)
                mock_warning.assert_called_with('Bad command, try again')

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

    class TestDataSaverSender:

        def setup_method(self):
            self.logger = setup_logger()
            self.global_shutdown_flag = threading.Event()
            self.azure_blob_parameters_with_key = (
                'DefaultEndpointsProtocol=https;AccountName=test_account;AccountKey=test_key;EndpointSuffix=core.windows.net'
            )
            self.azure_container_name = 'test_container'
            self.backblaze_access_key_id = 'test_access_key_id'
            self.backblaze_secret_access_key = 'test_secret_access_key'
            self.backblaze_endpoint_url = 'https://s3.eu-central-003.backblazeb2.com'
            self.backblaze_bucket_name = 'test_bucket'

            self.storage_connection_parameters = StorageConnectionParameters(
                self.azure_blob_parameters_with_key,
                self.azure_container_name,
                self.backblaze_access_key_id,
                self.backblaze_secret_access_key,
                self.backblaze_endpoint_url,
                self.backblaze_bucket_name
            )

            config_from_json = {
                "instruments": {
                    "spot": ["BTCUSDT"],
                    "usd_m_futures": ["BTCUSDT"],
                    "coin_m_futures": ["BTCUSDT_PERP"],
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "data_save_target": "json"
            }

            self.data_sink_config = DataSinkConfig(
                instruments={
                    'spot': config_from_json['instruments']['spot'],
                    'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                    'coin_m_futures': config_from_json['instruments']['coin_m_futures']
                },
                time_settings={
                    "file_duration_seconds": config_from_json["file_duration_seconds"],
                    "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                    "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
                },
                data_save_target=config_from_json['data_save_target']
            )

        def test_given_data_saver_when_run_then_stream_writers_are_started(self):
            queue_pool = DataSinkQueuePool()

            data_saver = StreamDataSaverAndSender(
                queue_pool=queue_pool,
                data_sink_config=self.data_sink_config,
                global_shutdown_flag=self.global_shutdown_flag
            )

            with patch.object(StreamDataSaverAndSender, 'start_stream_writer') as mock_start_stream_writer:
                data_saver.run()

                assert mock_start_stream_writer.call_count == len(
                    queue_pool.queue_lookup), "start_stream_writer should be called for each queue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_start_stream_writer_when_called_then_thread_is_started(self):
            queue_pool = DataSinkQueuePool()

            data_saver = StreamDataSaverAndSender(
                queue_pool=queue_pool,
                data_sink_config=self.data_sink_config,
                global_shutdown_flag=self.global_shutdown_flag
            )

            with patch('threading.Thread') as mock_thread:
                data_saver.start_stream_writer(
                    queue=queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM),
                    asset_parameters=AssetParameters(
                        market=Market.SPOT,
                        stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                        pairs=[]
                    )
                )

                mock_thread.assert_called_once()
                args, kwargs = mock_thread.call_args
                assert kwargs['target'] == data_saver._write_stream_to_target, "Thread target should be _stream_writer"
                mock_thread.return_value.start.assert_called_once()

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_stream_writer_when_shutdown_flag_set_then_exits_loop(self):

            queue_pool = DataSinkQueuePool()
            stream_listener_id = StreamId(pairs=['BTCUSDT'])

            queue = queue_pool.get_queue(market=Market.SPOT, stream_type=StreamType.DIFFERENCE_DEPTH_STREAM)

            queue.put_difference_depth_message(
                message='{"stream": "btcusdt@depth", "data": {}}',
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            asset_parameters = AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=['BTCUSDT']
            )

            data_saver = StreamDataSaverAndSender(
                queue_pool=queue_pool,
                data_sink_config=self.data_sink_config,
                global_shutdown_flag=self.global_shutdown_flag
            )

            with patch.object(StreamDataSaverAndSender, '_process_queue_data') as mock_process_queue_data, \
                    patch.object(StreamDataSaverAndSender, '_sleep_with_flag_check') as mock_sleep_with_flag_check:
                def side_effect(duration):
                    self.global_shutdown_flag.set()

                mock_sleep_with_flag_check.side_effect = side_effect

                data_saver._write_stream_to_target(queue=queue, asset_parameters=asset_parameters)

                assert mock_process_queue_data.call_count == 2, "Should process data during and after loop"
                mock_sleep_with_flag_check.assert_called_once_with(
                    self.data_sink_config.time_settings.file_duration_seconds)

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_process_queue_data_when_queue_is_empty_then_no_action_is_taken(self):
            queue_pool = DataSinkQueuePool()

            data_saver = StreamDataSaverAndSender(
                queue_pool=queue_pool,
                data_sink_config=self.data_sink_config,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue = queue_pool.get_queue(market=Market.SPOT, stream_type=StreamType.DIFFERENCE_DEPTH_STREAM)
            asset_parameters = AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=['BTCUSDT']
            )

            with patch.object(StreamDataSaverAndSender, 'write_data_to_json_file') as mock_write_json:
                data_saver._process_queue_data(queue=queue, asset_parameters=asset_parameters)
                mock_write_json.assert_not_called(), "Should not call write_data_to_json_file when queue is empty"

            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_process_queue_data_when_queue_has_data_then_data_is_processed(self, tmpdir):

            queue_pool = DataSinkQueuePool()

            data_saver = StreamDataSaverAndSender(
                queue_pool=queue_pool,
                data_sink_config=self.data_sink_config,
                global_shutdown_flag=self.global_shutdown_flag
            )

            stream_listener_id = StreamId(pairs=['BTCUSDT'])
            queue = queue_pool.get_queue(market=Market.SPOT, stream_type=StreamType.DIFFERENCE_DEPTH_STREAM)
            message = '{"stream":"btcusdt@depth","data":{}}'
            queue.currently_accepted_stream_id = stream_listener_id.id

            queue.put_difference_depth_message(
                message=message,
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            asset_parameters = AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=['BTCUSDT']
            )

            dump_dir = tmpdir.mkdir("dump")

            with patch.object(StreamDataSaverAndSender, 'write_data_to_json_file') as mock_write_json, \
                    patch.object(StreamDataSaverAndSender, 'write_data_to_zip_file') as mock_write_zip, \
                    patch.object(StreamDataSaverAndSender, 'send_zipped_json_to_azure_container') as mock_send_azure, \
                    patch.object(StreamDataSaverAndSender,
                                 'send_zipped_json_to_backblaze_bucket') as mock_send_backblaze:
                data_saver._process_queue_data(queue=queue, asset_parameters=asset_parameters)

                # Zakładamy, że konfiguracja wskazuje na docelowy zapis do pliku JSON
                assert mock_write_json.called, "write_data_to_json_file should be called"
                assert not mock_write_zip.called, "write_data_to_zip_file should not be called"
                assert not mock_send_azure.called, "send_zipped_json_to_azure_container should not be called"
                assert not mock_send_backblaze.called, "send_zipped_json_to_backblaze_bucket should not be called"

            DifferenceDepthQueue.clear_instances()

        def test_given_get_file_name_when_called_then_correct_format_is_returned(self):
            asset_parameters = AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=['BTCUSDT']
            )
            with patch(
                    'binance_archiver.timestamps_generator.TimestampsGenerator.get_utc_formatted_timestamp_for_file_name',
                    return_value='01-01-2022T00-00-00Z'):
                file_name = StreamDataSaverAndSender.get_file_name(asset_parameters)
                expected_file_name = "binance_difference_depth_stream_spot_btcusdt_01-01-2022T00-00-00Z"
                assert file_name == expected_file_name, "File name should be correctly formatted"

            DifferenceDepthQueue.clear_instances()

    class TestTimestampsGenerator:

        def test_given_time_utils_when_getting_utc_formatted_timestamp_then_format_is_correct(self):
            timestamp = TimestampsGenerator.get_utc_formatted_timestamp_for_file_name()
            pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z')
            assert re.match(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z', timestamp), \
                "Timestamp should match the format '%d-%m-%YT%H-%M-%SZ'"
            assert pattern.match(
                timestamp), f"Timestamp {timestamp} does not match the expected format %d-%m-%YT%H-%M-%SZ"

        def test_given_time_utils_when_getting_utc_timestamp_epoch_milliseconds_then_timestamp_is_accurate(self):
            timestamp_milliseconds_method = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            timestamp_milliseconds_now = round(datetime.now(timezone.utc).timestamp() * 1000)

            assert isinstance(timestamp_milliseconds_now, int), "Timestamp should be an integer"
            assert (abs(timestamp_milliseconds_method - timestamp_milliseconds_now) < 2000,
                    "The timestamp in milliseconds is not accurate or not in UTC.")

        def test_given_time_utils_when_getting_utc_timestamp_epoch_seconds_then_timestamp_is_accurate(self):
            timestamp_seconds_method = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            timestamp_seconds_now = round(datetime.now(timezone.utc).timestamp())

            assert (abs(timestamp_seconds_method - timestamp_seconds_now) < 2,
                    "The timestamp in seconds is not accurate or not in UTC.")

        def test_given_get_actual_epoch_timestamp_when_called_then_timestamps_are_in_utc(self):
            timestamp_seconds_method = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            timestamp_milliseconds_method = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()

            datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
            datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

            assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
            assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."

        def test_get_utc_timestamp_epoch_seconds_returns_int(self):
            timestamp = TimestampsGenerator.get_utc_timestamp_epoch_seconds()
            assert isinstance(timestamp, int), "Timestamp should be an integer"
