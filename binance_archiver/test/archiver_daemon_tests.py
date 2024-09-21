import re
import threading
import time
from datetime import datetime, timezone
from unittest.mock import patch
import pytest

from ..exceptions import (
    BadAzureParameters,
    BadConfigException,
    ClassInstancesAmountLimitException,
)

from ..archiver_daemon import (
    launch_data_sink,
    ArchiverFacade,
    StreamService,
    DataSaver,
    CommandLineInterface,
    QueuePool, TimeUtils
)

from ..setup_logger import setup_logger
from ..difference_depth_queue import DifferenceDepthQueue
from ..stream_id import StreamId
from ..trade_queue import TradeQueue
from ..market_enum import Market
from ..stream_type_enum import StreamType


class TestArchiverFacade:

    def test_init(self):
        assert True


    class TestArchiverFacade:

        def test_given_send_zip_to_blob_is_true_and_azure_blob_parameters_with_key_is_bad_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = ''
            container_name = 'some_container_name'

            with pytest.raises(BadAzureParameters) as excinfo:
                archiver_facade = launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

        def test_given_send_zip_to_blob_is_true_and_container_name_is_bad_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = ''

            with pytest.raises(BadAzureParameters) as excinfo:
                archiver_facade = launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

        def test_given_archiver_facade_when_init_then_global_shutdown_flag_is_false(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 60,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            logger = setup_logger()
            archiver_facade = ArchiverFacade(config=config, logger=logger)

            assert not archiver_facade.global_shutdown_flag.is_set()
            TradeQueue.clear_instances()
            DifferenceDepthQueue.clear_instances()

        def test_given_archiver_facade_when_init_then_queues_are_set_properly(self):
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

            logger = setup_logger()
            archiver_facade = ArchiverFacade(config=config, logger=logger)

            queue_pool = archiver_facade.queue_pool

            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_run_call_when_threads_invoked_then_correct_threads_are_started(self):

            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_facade = launch_data_sink(config)

            time.sleep(3)

            num_markets = len(config["instruments"])

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
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH}' in thread_names
                assert f'stream_service: market: {Market[market]}, stream_type: {StreamType.TRADE}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.DIFFERENCE_DEPTH}' in thread_names
                assert f'stream_writer: market: {Market[market]}, stream_type: {StreamType.TRADE}' in thread_names
                assert f'snapshot_daemon: market: {Market[market]}' in thread_names

            assert len(daemon_threads) == total_expected_threads

            archiver_facade.shutdown()

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_archiver_facade_when_shutdown_called_then_no_threads_are_left(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 5,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_facade = launch_data_sink(config)

            time.sleep(15)

            archiver_facade.shutdown()

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

            assert len(active_threads) == 0, f"Still active threads after shutdown: {[thread.name for thread in active_threads]}"

            del archiver_facade

        @pytest.mark.parametrize('execution_number', range(2))
        def test_given_archiver_daemon_when_shutdown_method_during_no_stream_switch_is_called_then_no_threads_are_left(self,execution_number):
            config = {
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
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_sink(config)

            time.sleep(15)

            archiver_daemon.shutdown()

            active_threads = []

            for _ in range(20):
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

        @pytest.mark.parametrize('execution_number', range(2))
        def test_given_archiver_daemon_when_shutdown_method_during_stream_switch_is_called_then_no_threads_are_left(self,execution_number):
            config = {
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
                "websocket_life_time_seconds": 5,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }

            archiver_daemon = launch_data_sink(config)

            time.sleep(5)

            archiver_daemon.shutdown()

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

            assert len(active_threads) == 0, (f"Still active threads after run {execution_number + 1}"
                                              f": {[thread.name for thread in active_threads]}")

            del archiver_daemon


    class TestQueuePool:

        def test_given_queue_pool_when_initialized_then_queues_are_set_properly(self):
            queue_pool = QueuePool()

            expected_keys = [
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH),
                (Market.SPOT, StreamType.TRADE),
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH),
                (Market.USD_M_FUTURES, StreamType.TRADE),
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH),
                (Market.COIN_M_FUTURES, StreamType.TRADE)
            ]

            assert set(queue_pool.queue_lookup.keys()) == set(
                expected_keys), "queue_lookup keys do not match expected keys"

            assert isinstance(queue_pool.spot_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.usd_m_futures_orderbook_stream_message_queue, DifferenceDepthQueue)
            assert isinstance(queue_pool.coin_m_orderbook_stream_message_queue, DifferenceDepthQueue)

            assert isinstance(queue_pool.spot_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.usd_m_futures_trade_stream_message_queue, TradeQueue)
            assert isinstance(queue_pool.coin_m_trade_stream_message_queue, TradeQueue)

            assert len(TradeQueue._instances) == 3
            assert len(DifferenceDepthQueue._instances) == 3

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_get_queue_called_then_returns_correct_queue(self):
            queue_pool = QueuePool()

            expected_queues = {
                (Market.SPOT, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.SPOT, StreamType.TRADE): TradeQueue,
                (Market.USD_M_FUTURES, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.USD_M_FUTURES, StreamType.TRADE): TradeQueue,
                (Market.COIN_M_FUTURES, StreamType.DIFFERENCE_DEPTH): DifferenceDepthQueue,
                (Market.COIN_M_FUTURES, StreamType.TRADE): TradeQueue
            }

            for (market, stream_type), expected_queue_type in expected_queues.items():
                queue = queue_pool.get_queue(market, stream_type)
                assert isinstance(queue, expected_queue_type)
                assert queue.market == market

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_trade_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePool()

            queue_pool.fourth_trade_queue = TradeQueue(market=Market.SPOT)

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fifth_trade_queue = TradeQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 4 instances of TradeQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_queue_pool_when_more_than_allowed_difference_depth_queues_created_then_exception_is_thrown(self):
            queue_pool = QueuePool()

            queue_pool.fourth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            with pytest.raises(ClassInstancesAmountLimitException) as excinfo:
                queue_pool.fifth_difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

            assert str(excinfo.value) == "Cannot create more than 4 instances of DifferenceDepthQueue"

            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestStreamService:

        def test(self):
            ...


    class TestSnapshotManager:

        def test(self):
            ...


    class TestCommandLineInterface:

        def test_given_modify_subscription_when_adding_asset_then_asset_is_added_to_instruments(self):
            instruments = {
                'spot': ['BTCUSDT', 'ETHUSDT'],
                'usd_m_futures': ['BTCUSDT'],
            }
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePool()
            stream_service = StreamService(
                instruments=instruments,
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )
            cli = CommandLineInterface(
                instruments=instruments,
                logger=logger,
                stream_service=stream_service
            )

            message = {'modify_subscription': {'type': 'subscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' in instruments['spot'], "Asset not added to instruments"
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()

        def test_given_modify_subscription_when_removing_asset_then_asset_is_removed_from_instruments(self):
            instruments = {
                'spot': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],
                'usd_m_futures': ['BTCUSDT'],
            }
            logger = setup_logger()
            global_shutdown_flag = threading.Event()
            queue_pool = QueuePool()
            stream_service = StreamService(
                instruments=instruments,
                logger=logger,
                queue_pool=queue_pool,
                global_shutdown_flag=global_shutdown_flag
            )
            cli = CommandLineInterface(
                instruments=instruments,
                logger=logger,
                stream_service=stream_service
            )

            message = {'modify_subscription': {'type': 'unsubscribe', 'market': 'spot', 'asset': 'BNBUSDT'}}
            cli.handle_command(message)

            assert 'BNBUSDT' not in instruments['spot'], "Asset not removed from instruments"
            DifferenceDepthQueue.clear_instances()
            TradeQueue.clear_instances()


    class TestDataSaver:

        def setup_method(self):
            self.logger = setup_logger()
            self.global_shutdown_flag = threading.Event()
            self.azure_blob_parameters_with_key = ('DefaultEndpointsProtocol=https;'
                                                   'AccountName=test_account;AccountKey=test_key;'
                                                   'EndpointSuffix=core.windows.net')
            self.container_name = 'test_container'

        def test_given_blob_parameters_when_initializing_then_blob_service_client_is_initialized(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=self.azure_blob_parameters_with_key,
                container_name=self.container_name,
                global_shutdown_flag=self.global_shutdown_flag
            )

            assert data_saver.blob_service_client is not None, "BlobServiceClient should be initialized"
            assert data_saver.container_name == self.container_name, "Container name should be set"

        def test_given_no_blob_parameters_when_initializing_then_blob_service_client_is_none(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            assert data_saver.blob_service_client is None, "BlobServiceClient should be None when parameters are missing"

        def test_given_data_saver_when_run_then_stream_writers_are_started(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue_pool = QueuePool()
            with patch.object(data_saver, 'start_stream_writer') as mock_start_stream_writer:
                data_saver.run_data_saver(
                    queue_pool=queue_pool,
                    dump_path='dump/',
                    file_duration_seconds=60,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                assert mock_start_stream_writer.call_count == len(
                    queue_pool.queue_lookup), "start_stream_writer should be called for each queue"

        def test_given_start_stream_writer_when_called_then_thread_is_started(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue = DifferenceDepthQueue(market=Market.SPOT)
            with patch('threading.Thread') as mock_thread:
                data_saver.start_stream_writer(
                    queue=queue,
                    market=Market.SPOT,
                    file_duration_seconds=60,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                mock_thread.assert_called_once()
                args, kwargs = mock_thread.call_args
                assert kwargs['target'] == data_saver._stream_writer, "Thread target should be _stream_writer"
                mock_thread.return_value.start.assert_called_once()
            DifferenceDepthQueue.clear_instances()

        def test_given_stream_writer_when_shutdown_flag_set_then_exits_loop(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            stream_listener_id = StreamId(pairs=['BTCUSDT'])

            queue = DifferenceDepthQueue(market=Market.SPOT)
            queue.put_queue_message(
                message='{"stream": "btcusdt@depth", "data": {}}',
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            with patch.object(data_saver, '_process_stream_data') as mock_process_stream_data, \
                    patch.object(data_saver, '_sleep_with_flag_check') as mock_sleep_with_flag_check:

                def side_effect(duration):
                    self.global_shutdown_flag.set()

                mock_sleep_with_flag_check.side_effect = side_effect

                data_saver._stream_writer(
                    queue=queue,
                    market=Market.SPOT,
                    file_duration_seconds=1,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                assert mock_process_stream_data.call_count == 2, "Should process data during and after loop"
                mock_sleep_with_flag_check.assert_called_once_with(1)
            DifferenceDepthQueue.clear_instances()

        def test_given_process_stream_data_when_queue_is_empty_then_no_action_is_taken(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            queue = DifferenceDepthQueue(market=Market.SPOT)
            with patch.object(data_saver, 'save_to_json') as mock_save_to_json:
                data_saver._process_stream_data(
                    queue=queue,
                    market=Market.SPOT,
                    dump_path='dump/',
                    stream_type=StreamType.DIFFERENCE_DEPTH,
                    save_to_json=True,
                    save_to_zip=False,
                    send_zip_to_blob=False
                )

                mock_save_to_json.assert_not_called(), "Should not call _save_to_json when queue is empty"
            DifferenceDepthQueue.clear_instances()

        def test_given_process_stream_data_when_queue_has_data_then_data_is_processed(self, tmpdir):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            stream_listener_id = StreamId(pairs=['BTCUSDT'])

            queue = DifferenceDepthQueue(market=Market.SPOT)
            message = '{"stream": "btcusdt@depth", "data": {}}'

            queue.currently_accepted_stream_id = stream_listener_id.id

            queue.put_queue_message(
                message=message,
                stream_listener_id=stream_listener_id,
                timestamp_of_receive=1234567890
            )

            dump_path = tmpdir.mkdir("dump")

            with patch.object(data_saver, 'save_to_json') as mock_save_to_json, \
                    patch.object(data_saver, 'save_to_zip') as mock_save_to_zip, \
                    patch.object(data_saver, 'send_zipped_json_to_blob') as mock_send_zip:
                data_saver._process_stream_data(
                    queue=queue,
                    market=Market.SPOT,
                    dump_path=str(dump_path),
                    stream_type=StreamType.DIFFERENCE_DEPTH,
                    save_to_json=True,
                    save_to_zip=True,
                    send_zip_to_blob=True
                )

                assert mock_save_to_json.called, "save_to_json should be called"
                assert mock_save_to_zip.called, "save_to_zip should be called"
                assert mock_send_zip.called, "send_zipped_json_to_blob should be called"
            DifferenceDepthQueue.clear_instances()

        def test_given_get_file_name_when_called_then_correct_format_is_returned(self):
            data_saver = DataSaver(
                logger=self.logger,
                azure_blob_parameters_with_key=None,
                container_name=None,
                global_shutdown_flag=self.global_shutdown_flag
            )

            pair = "BTCUSDT"
            market = Market.SPOT
            stream_type = StreamType.DIFFERENCE_DEPTH

            with patch('binance_archiver.archiver_daemon.TimeUtils.get_utc_formatted_timestamp',
                       return_value='01-01-2022T00-00-00Z'):
                file_name = data_saver.get_file_name(pair, market, stream_type)

                expected_prefix = "binance_difference_depth"
                expected_market_name = "spot"
                expected_file_name = f"{expected_prefix}_{expected_market_name}_{pair.lower()}_01-01-2022T00-00-00Z.json"
                assert file_name == expected_file_name, "File name should be correctly formatted"
            DifferenceDepthQueue.clear_instances()


    class TestTimeUtils:

        def test_given_time_utils_when_getting_utc_formatted_timestamp_then_format_is_correct(self):
            timestamp = TimeUtils.get_utc_formatted_timestamp()
            pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z')
            assert pattern.match(timestamp), f"Timestamp {timestamp} does not match the expected format %d-%m-%YT%H-%M-%SZ"

        def test_given_time_utils_when_getting_utc_timestamp_epoch_milliseconds_then_timestamp_is_accurate(self):
            timestamp_milliseconds_method = TimeUtils.get_utc_timestamp_epoch_milliseconds()
            timestamp_milliseconds_now = round(datetime.now(timezone.utc).timestamp() * 1000)

            assert (abs(timestamp_milliseconds_method - timestamp_milliseconds_now) < 2000,
                    "The timestamp in milliseconds is not accurate or not in UTC.")

        def test_given_time_utils_when_getting_utc_timestamp_epoch_seconds_then_timestamp_is_accurate(self):
            timestamp_seconds_method = TimeUtils.get_utc_timestamp_epoch_seconds()
            timestamp_seconds_now = round(datetime.now(timezone.utc).timestamp())

            assert (abs(timestamp_seconds_method - timestamp_seconds_now) < 2,
                    "The timestamp in seconds is not accurate or not in UTC.")

        def test_given_get_actual_epoch_timestamp_when_called_then_timestamps_are_in_utc(self):
            timestamp_seconds_method = TimeUtils.get_utc_timestamp_epoch_seconds()
            timestamp_milliseconds_method = TimeUtils.get_utc_timestamp_epoch_milliseconds()

            datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
            datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

            assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
            assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."


    class TestLaunchDataSink:

        def test_given_config_has_no_instrument_then_exception_is_thrown(self):

            config = {
                "instruments": {},
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Instruments config is missing or not a dictionary."

        def test_given_market_type_is_empty_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": [],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Pairs for market spot are missing or invalid."

        def test_given_too_many_markets_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "spot": ["BTCUSDT", "ETHUSDT"],
                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"],
                    "actions": ["AAPL", "TSLA"],
                    "mining": ["BTCMINING"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Config must contain 1 to 3 markets."

        def test_given_not_handled_market_type_then_exception_is_thrown(self):
            config = {
                "instruments": {
                    "mining": ["BTCMINING"]
                },
                "file_duration_seconds": 30,
                "snapshot_fetcher_interval_seconds": 60,
                "websocket_life_time_seconds": 70,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }

            azure_blob_parameters_with_key = 'DefaultEndpointsProtocol=...'
            container_name = 'some_container_name'

            with pytest.raises(BadConfigException) as excinfo:
                launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
                )

            assert str(excinfo.value) == "Invalid or not handled market: mining"
