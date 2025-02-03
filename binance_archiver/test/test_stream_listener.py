import logging
import time
import threading
import pytest
import re

from binance_archiver import DataSinkConfig
from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.queue_pool import DataSinkQueuePool
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.setup_logger import setup_logger
from binance_archiver.stream_id import StreamId
from binance_archiver.stream_listener import StreamListener, WrongListInstanceException, PairsLengthException
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.blackout_supervisor import BlackoutSupervisor
from binance_archiver.trade_queue import TradeQueue


class TestStreamListener:

    def test_given_stream_listener_when_init_then_stream_listener_initializes_with_valid_parameters(self):
        config_from_json = {
            "instruments": {
                "spot": [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
                    "ADAUSDT", "SHIBUSDT", "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"
                ],
                "usd_m_futures": [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
                    "ADAUSDT", "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"
                ],
                "coin_m_futures": [
                    "BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                    "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                    "DOTUSD_PERP"
                ]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 60,
            "websocket_life_time_seconds": 5,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        setup_logger()
        global_shutdown_flag = threading.Event()

        queue_pool = DataSinkQueuePool()

        for market in Market:

            pairs = data_sink_config.instruments.get_pairs(market=market)

            difference_depth_stream_listener = StreamListener(
                queue=queue_pool.get_queue(market, StreamType.DIFFERENCE_DEPTH_STREAM),
                asset_parameters=AssetParameters(
                    market=market,
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    pairs=pairs
                )
            )

            expected_pairs_amount = len(pairs)

            assert difference_depth_stream_listener._ws is None
            assert isinstance(difference_depth_stream_listener.id, StreamId)
            assert difference_depth_stream_listener.id.pairs_amount == expected_pairs_amount

            trade_stream_listener = StreamListener(
                queue=queue_pool.get_queue(market, StreamType.TRADE_STREAM),
                asset_parameters=AssetParameters(
                    market=market,
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    pairs=pairs
                )
            )

            assert difference_depth_stream_listener._ws is None
            assert isinstance(trade_stream_listener.id, StreamId)
            assert trade_stream_listener.id.pairs_amount == expected_pairs_amount

            trade_stream_listener.close_websocket_app()
            difference_depth_stream_listener.close_websocket_app()

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_argument_as_str_then_exception_is_thrown(self):
        config_from_json = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        setup_logger()
        queue_pool = DataSinkQueuePool()

        with pytest.raises(WrongListInstanceException) as excinfo:
            queue = queue_pool.get_queue(Market.SPOT, StreamType.DIFFERENCE_DEPTH_STREAM)
            stream_listener = StreamListener(
                queue=queue_pool.get_queue(market=Market.SPOT, stream_type=StreamType.DIFFERENCE_DEPTH_STREAM),
                asset_parameters=AssetParameters(
                    market=Market.SPOT,
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)[0]
                )
            )

        assert str(excinfo.value) == "pairs argument is not a list"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_amount_of_zero_then_exception_is_thrown(self):
        config_from_json = {
            "instruments": {
                "spot": [],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        setup_logger()
        queue_pool = DataSinkQueuePool()

        with pytest.raises(PairsLengthException) as excinfo:
            queue = queue_pool.get_queue(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM
            )
            stream_listener = StreamListener(
                queue=queue,
                asset_parameters=AssetParameters(
                    market=Market.SPOT,
                    stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                    pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
                )
            )

        assert str(excinfo.value) == "pairs len is zero"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_open_then_message_is_being_logged(self, caplog):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(
            queue=queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=pairs
            )
        )

        with caplog.at_level(logging.INFO):
            stream_listener.start_websocket_app()

        assert "Starting streamListener" in caplog.text

        stream_listener.close_websocket_app()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_close_then_close_message_is_being_logged(self, caplog):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(
            queue=queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=pairs
            )
        )

        with caplog.at_level(logging.INFO):
            stream_listener.close_websocket_app()

        assert "Closing StreamListener" in caplog.text

        stream_listener.close_websocket_app()
        stream_listener._blackout_supervisor.shutdown_supervisor()

        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_connected_then_error_is_being_logged(self, caplog):
        ...

    def test_given_trade_stream_listener_when_connected_then_message_is_correctly_passed_to_trade_queue(self):

        logger = setup_logger()
        config_from_json = {
            "instruments": {
                "spot": ["BTCUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.TRADE_STREAM,
                pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
            )
        )

        trade_queue.currently_accepted_stream_id = trade_stream_listener.id

        trade_stream_listener.start_websocket_app()

        time.sleep(5)

        trade_stream_listener.close_websocket_app()

        assert trade_queue.qsize() > 0

        sample_message = trade_queue.get_nowait()
        sample_message_str_from_queue = sample_message

        assert re.search(r'"stream"\s*:\s*"[^"]*@trade"',sample_message_str_from_queue), "Stream should contain '@trade'."
        assert re.search(r'"e"\s*:\s*"trade"', sample_message_str_from_queue), "Event type should be 'trade'."
        assert re.search(r'"s"\s*:\s*"BTCUSDT"', sample_message_str_from_queue), "Symbol should be 'BTCUSDT'."

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_connected_then_message_is_correctly_passed_to_diff_queue(self):
        setup_logger()

        config_from_json = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 10,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_stream_listener = StreamListener(
            queue=difference_depth_queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
            )
        )

        difference_depth_queue.currently_accepted_stream_id = difference_depth_stream_listener.id.id

        difference_depth_stream_listener.start_websocket_app()

        time.sleep(10)

        difference_depth_stream_listener.close_websocket_app()

        sample_message = difference_depth_queue.queue.get_nowait()

        assert difference_depth_queue.qsize() > 0

        assert re.search(r'"stream"\s*:\s*"[^"]*@depth[^"]*"', sample_message), "Stream powinien zawierać '@depth'."
        assert re.search(r'"e"\s*:\s*"depthUpdate"', sample_message), "Typ zdarzenia powinien być 'depthUpdate'."
        assert re.search(r'"s"\s*:\s*"(BTCUSDT|ETHUSDT)"', sample_message), "Symbol powinien być 'BTCUSDT' lub 'ETHUSDT'."

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):
        logger = setup_logger()

        from unittest.mock import patch

        config_from_json = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.TRADE_STREAM,
                pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
            )
        )

        trade_queue.currently_accepted_stream_id = trade_stream_listener.id

        with patch.object(StreamListener, 'restart_websocket_app') as mock_restart, \
                patch.object(BlackoutSupervisor, 'notify') as mock_notify:
            trade_stream_listener.start_websocket_app()

            time.sleep(10)

            mock_notify.assert_called()

            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.TRADE_STREAM} {Market.SPOT}'

        assert trade_stream_listener._blackout_supervisor is not None, (
            "Supervisor should be instantiated within StreamListener"
        )
        assert isinstance(trade_stream_listener._blackout_supervisor, BlackoutSupervisor)
        for name_ in active_threads:
            print(name_)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 3
        trade_stream_listener.close_websocket_app()
        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):

        from unittest.mock import patch

        config_from_json = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 100,
            "save_to_json": False,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        data_sink_config = DataSinkConfig(
            instruments={
                'spot': config_from_json['instruments']['spot'],
                'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
                'coin_m_futures': config_from_json['instruments']['coin_m_futures'],
            },
            time_settings={
                "file_duration_seconds": config_from_json["file_duration_seconds"],
                "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
                "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
            }
        )

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            asset_parameters=AssetParameters(
                market=Market.SPOT,
                stream_type=StreamType.DIFFERENCE_DEPTH_STREAM,
                pairs=data_sink_config.instruments.get_pairs(market=Market.SPOT)
            )
        )

        with patch.object(StreamListener, 'restart_websocket_app') as mock_restart, \
                patch.object(BlackoutSupervisor, 'notify') as mock_notify:
            difference_depth_queue_listener.start_websocket_app()

            time.sleep(10)
            mock_notify.assert_called()

            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.DIFFERENCE_DEPTH_STREAM} {Market.SPOT}'

        assert difference_depth_queue_listener._blackout_supervisor is not None, "Supervisor should be instantiated within StreamListener"
        assert isinstance(difference_depth_queue_listener._blackout_supervisor, BlackoutSupervisor)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 3

        difference_depth_queue_listener.close_websocket_app()

        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        ...

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        ...

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_received_then_supervisor_is_notified(self):
        ...

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_received_then_supervisor_is_notified(self):
        ...

    def test_given_trade_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...

    def test_given_difference_depth_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...
