import json
import time
import threading

import pytest
import websocket

from binance_archiver import ArchiverDaemon
from binance_archiver.orderbook_level_2_listener.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.setup_logger import setup_logger
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.stream_listener import StreamListener, WrongListInstanceException, \
    PairsLengthException
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType
from binance_archiver.orderbook_level_2_listener.blackoutsupervisor import BlackoutSupervisor
from binance_archiver.orderbook_level_2_listener.trade_queue import TradeQueue


class TestStreamListener:

    def test_given_stream_listener_when_init_then_stream_listener_initializes_with_valid_parameters(self):
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

        logger = setup_logger()
        archiver_daemon = ArchiverDaemon(logger)

        spot_difference_depth_stream_listener = StreamListener(
            queue=archiver_daemon.spot_orderbook_stream_message_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.SPOT
        )

        spot_trade_stream_listener = StreamListener(
            queue=archiver_daemon.spot_transaction_stream_message_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        assert isinstance(spot_difference_depth_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(spot_difference_depth_stream_listener.id, StreamId)
        assert spot_difference_depth_stream_listener.pairs_amount == 12
        assert spot_difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"

        assert isinstance(spot_trade_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(spot_trade_stream_listener.id, StreamId)
        assert spot_trade_stream_listener.pairs_amount == 12
        assert spot_trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", "on_message should be assigned to _on_trade_message when stream_type is TRADE"

        spot_trade_stream_listener.websocket_app.close()
        spot_trade_stream_listener._blackout_supervisor.shutdown_supervisor()
        spot_difference_depth_stream_listener.websocket_app.close()
        spot_difference_depth_stream_listener._blackout_supervisor.shutdown_supervisor()

        usd_m_futures_difference_depth_stream_listener = StreamListener(
            queue=archiver_daemon.usd_m_futures_orderbook_stream_message_queue,
            pairs=config['instruments']['usd_m_futures'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.USD_M_FUTURES
        )

        usd_m_futures_trade_stream_listener = StreamListener(
            queue=archiver_daemon.usd_m_futures_transaction_stream_message_queue,
            pairs=config['instruments']['usd_m_futures'],
            stream_type=StreamType.TRADE,
            market=Market.USD_M_FUTURES
        )

        assert isinstance(usd_m_futures_difference_depth_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(usd_m_futures_difference_depth_stream_listener.id, StreamId)
        assert usd_m_futures_difference_depth_stream_listener.pairs_amount == 11
        assert usd_m_futures_difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"

        assert isinstance(usd_m_futures_trade_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(usd_m_futures_trade_stream_listener.id, StreamId)
        assert usd_m_futures_trade_stream_listener.pairs_amount == 11
        assert usd_m_futures_trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", "on_message should be assigned to _on_trade_message when stream_type is TRADE"

        usd_m_futures_difference_depth_stream_listener.websocket_app.close()
        usd_m_futures_difference_depth_stream_listener._blackout_supervisor.shutdown_supervisor()
        usd_m_futures_trade_stream_listener.websocket_app.close()
        usd_m_futures_trade_stream_listener._blackout_supervisor.shutdown_supervisor()

        coin_m_futures_difference_depth_stream_listener = StreamListener(
            queue=archiver_daemon.coin_m_orderbook_stream_message_queue,
            pairs=config['instruments']['coin_m_futures'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.COIN_M_FUTURES
        )

        coin_m_futures_trade_stream_listener = StreamListener(
            queue=archiver_daemon.coin_m_transaction_stream_message_queue,
            pairs=config['instruments']['coin_m_futures'],
            stream_type=StreamType.TRADE,
            market=Market.COIN_M_FUTURES
        )

        assert isinstance(coin_m_futures_difference_depth_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(coin_m_futures_difference_depth_stream_listener.id, StreamId)
        assert coin_m_futures_difference_depth_stream_listener.pairs_amount == 11
        assert coin_m_futures_difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"

        assert isinstance(coin_m_futures_trade_stream_listener.websocket_app, websocket.WebSocketApp)
        assert isinstance(coin_m_futures_trade_stream_listener.id, StreamId)
        assert coin_m_futures_trade_stream_listener.pairs_amount == 11
        assert coin_m_futures_trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", "on_message should be assigned to _on_trade_message when stream_type is TRADE"

        coin_m_futures_difference_depth_stream_listener.websocket_app.close()
        coin_m_futures_difference_depth_stream_listener._blackout_supervisor.shutdown_supervisor()
        coin_m_futures_trade_stream_listener.websocket_app.close()
        coin_m_futures_trade_stream_listener._blackout_supervisor.shutdown_supervisor()

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_argument_as_str_then_exception_is_thrown(self):
        config = {
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

        logger = setup_logger()
        archiver_daemon = ArchiverDaemon(logger)

        with pytest.raises(WrongListInstanceException) as excinfo:
            stream_listener = StreamListener(
                queue=archiver_daemon.spot_orderbook_stream_message_queue,
                pairs=config['instruments']['spot'][0],
                stream_type=StreamType.DIFFERENCE_DEPTH,
                market=Market.SPOT
            )

        assert str(excinfo.value) == "pairs argument is not a list"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_stream_listener_when_init_with_pairs_amount_of_zero_then_exception_is_thrown(self):
        config = {
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

        logger = setup_logger()
        archiver_daemon = ArchiverDaemon(logger)

        with pytest.raises(PairsLengthException) as excinfo:
            stream_listener = StreamListener(
                queue=archiver_daemon.spot_orderbook_stream_message_queue,
                pairs=config['instruments']['spot'],
                stream_type=StreamType.DIFFERENCE_DEPTH,
                market=Market.SPOT
            )

        assert str(excinfo.value) == "pairs len is zero"

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_open_then_message_is_being_logged(self, capsys):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(queue=queue, pairs=pairs, stream_type=StreamType.TRADE, market=Market.SPOT)
        stream_listener.websocket_app.on_open(stream_listener.websocket_app)
        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()

        captured = capsys.readouterr()
        assert "WebSocket connection opened" in captured.out

        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_on_close_then_close_message_is_being_logged(self, capsys):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(queue=queue, pairs=pairs, stream_type=StreamType.TRADE, market=Market.SPOT)

        close_status_code = 1000
        close_msg = "Normal closure"
        stream_listener.websocket_app.on_close(stream_listener.websocket_app, close_status_code, close_msg)
        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()

        captured = capsys.readouterr()
        assert "WebSocket connection closed" in captured.out
        assert close_msg in captured.out
        assert str(close_status_code) in captured.out

        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_connected_then_error_is_being_logged(self, capsys):
        pairs = ['BTCUSDT']
        queue = TradeQueue(market=Market.SPOT)
        stream_listener = StreamListener(queue=queue, pairs=pairs, stream_type=StreamType.TRADE, market=Market.SPOT)

        error_message = "Test error"
        stream_listener.websocket_app.on_error(stream_listener.websocket_app, error_message)
        stream_listener.websocket_app.close()
        stream_listener._blackout_supervisor.shutdown_supervisor()
        TradeQueue.clear_instances()

        captured = capsys.readouterr()
        assert error_message in captured.out

    def test_given_trade_stream_listener_when_connected_then_message_is_correctly_passed_to_trade_queue(self):
        config = {
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

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        trade_stream_listener_thread = threading.Thread(
            target=trade_stream_listener.websocket_app.run_forever,
            daemon=True
        )

        trade_stream_listener_thread.start()

        time.sleep(5)

        trade_stream_listener.websocket_app.close()

        sample_message = trade_queue.get_nowait()
        print(f'sample_message {sample_message[0]} {type(sample_message[0])}')
        sample_message_dict = json.loads(sample_message[0])

        print(trade_queue.qsize())

        assert trade_stream_listener.websocket_app.on_message.__name__ == "_on_trade_message", \
            "on_message should be assigned to _on_trade_message when stream_type is TRADE"
        assert trade_queue.qsize() > 0
        assert sample_message_dict['stream'].split('@')[1] == 'trade'
        assert sample_message_dict['data']['e'] == 'trade'

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_connected_then_message_is_correctly_passed_to_diff_queue(self):
        config = {
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

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_stream_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.SPOT
        )

        difference_depth_queue.currently_accepted_stream_id = difference_depth_stream_listener.id.id

        difference_depth_stream_listener_thread = threading.Thread(
            target=difference_depth_stream_listener.websocket_app.run_forever,
            daemon=True
        )

        difference_depth_stream_listener_thread.start()

        time.sleep(10)

        difference_depth_stream_listener.websocket_app.close()

        sample_message = difference_depth_queue.queue.get_nowait()
        sample_message_dict = json.loads(sample_message[0])

        assert difference_depth_stream_listener.websocket_app.on_message.__name__ == "_on_difference_depth_message", \
            "on_message should be assigned to _on_difference_depth_message when stream_type is DIFFERENCE_DEPTH"
        assert difference_depth_queue.qsize() > 0
        assert sample_message_dict['stream'].split('@')[1] == 'depth'
        assert sample_message_dict['data']['e'] == 'depthUpdate'

        DifferenceDepthQueue.clear_instances()
        TradeQueue.clear_instances()

    def test_given_trade_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):
        from unittest.mock import patch

        config = {
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

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        with patch.object(trade_stream_listener, 'restart_websocket_app') as mock_restart, \
                patch.object(trade_stream_listener._blackout_supervisor, 'notify') as mock_notify:
            trade_stream_listener.start_websocket_app()

            time.sleep(10)
            mock_notify.assert_called()
            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.TRADE} {Market.SPOT}'

        assert trade_stream_listener._blackout_supervisor is not None, "Supervisor should be instantiated within StreamListener"
        assert isinstance(trade_stream_listener._blackout_supervisor, BlackoutSupervisor)

        print('hujiksde')
        for name_ in active_threads:
            print(name_)

        assert presumed_thread_name in active_threads
        assert len(active_threads) == 2

        trade_stream_listener.websocket_app.close()

        TradeQueue.clear_instances()

    def test_given_difference_depth_stream_listener_when_init_then_supervisor_starts_correctly_and_is_being_notified(self):
        from unittest.mock import patch

        config = {
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

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.SPOT
        )

        with patch.object(difference_depth_queue_listener, 'restart_websocket_app') as mock_restart, \
                patch.object(difference_depth_queue_listener._blackout_supervisor, 'notify') as mock_notify:
            difference_depth_queue_listener.start_websocket_app()

            time.sleep(10)
            mock_notify.assert_called()

            mock_restart.assert_not_called()

        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.DIFFERENCE_DEPTH} {Market.SPOT}'

        assert difference_depth_queue_listener._blackout_supervisor is not None, "Supervisor should be instantiated within StreamListener"
        assert isinstance(difference_depth_queue_listener._blackout_supervisor, BlackoutSupervisor)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 2

        difference_depth_queue_listener.websocket_app.close()

        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        config = {
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

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        trade_stream_listener_thread = threading.Thread(target=trade_stream_listener.websocket_app.run_forever,
                                                        daemon=True)
        trade_stream_listener_thread.start()

        time.sleep(5)

        trade_stream_listener.websocket_app.on_message = None
        time.sleep(12)

        assert trade_stream_listener.supervisor_signal_shutdown_flag.is_set(), \
            "Supervisor's shutdown signal should be set after a period without incoming messages."

        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_income_stops_then_supervisors_sets_flag_to_stop(self):
        config = {
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

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.SPOT
        )

        difference_depth_stream_listener_thread = threading.Thread(
            target=difference_depth_queue_listener.websocket_app.run_forever,
            daemon=True)
        difference_depth_stream_listener_thread.start()

        time.sleep(5)

        difference_depth_queue_listener.websocket_app.on_message = None
        time.sleep(12)

        # Check if the supervisor's shutdown flag is set due to no incoming messages
        assert difference_depth_queue_listener.supervisor_signal_shutdown_flag.is_set(), \
            "Supervisor's shutdown signal should be set after a period without incoming messages."

        # Clean up
        difference_depth_queue_listener.websocket_app.close()
        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_trade_stream_listener_when_message_received_then_supervisor_is_notified(self):
        from unittest.mock import patch

        config = {
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

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        with patch.object(trade_stream_listener._blackout_supervisor, 'notify') as mock_notify:
            simulated_message = '{"e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 12345, "p": "0.001", "q": "100", "b": 88, "a": 50, "T": 123456785, "m": True, "M": True}'

            trade_stream_listener.websocket_app.on_message(trade_stream_listener.websocket_app, simulated_message)

            mock_notify.assert_called_once()

        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_difference_depth_stream_listener_when_message_received_then_supervisor_is_notified(self):
        from unittest.mock import patch

        config = {
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

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        difference_depth_queue_listener = StreamListener(
            queue=difference_depth_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.DIFFERENCE_DEPTH,
            market=Market.SPOT
        )

        with patch.object(difference_depth_queue_listener._blackout_supervisor, 'notify') as mock_notify:
            simulated_message = '{"e": "depthUpdate", "E": 123456789, "s": "BTCUSDT", "U": 157, "u": 160, "b": [["0.0024", "10"]], "a": [["0.0026", "100"]]}'

            difference_depth_queue_listener.websocket_app.on_message(difference_depth_queue_listener.websocket_app,
                                                                     simulated_message)

            mock_notify.assert_called_once()

        difference_depth_queue_listener.websocket_app.close()
        DifferenceDepthQueue.clear_instances()

    def test_given_trade_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...

    def test_given_difference_depth_stream_listener_when_websocket_app_close_then_supervisor_is_properly_stopped(self):
        ...


class TestOther:

    def test_non_unittest_test_given_trade_stream_listener_when_init_then_supervisor_starts_correctly(self):
        # Konfiguracja
        config = {
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

        trade_queue = TradeQueue(market=Market.SPOT)

        trade_stream_listener = StreamListener(
            queue=trade_queue,
            pairs=config['instruments']['spot'],
            stream_type=StreamType.TRADE,
            market=Market.SPOT
        )

        # Ręczne mockowanie metod
        original_restart_websocket_app = trade_stream_listener.restart_websocket_app
        trade_stream_listener.restart_websocket_app = lambda: print("Mocked restart_websocket_app called")

        original_notify = trade_stream_listener._blackout_supervisor.notify
        notify_called = False

        def mock_notify():
            nonlocal notify_called
            notify_called = True
            print("Mocked notify called")

        trade_stream_listener._blackout_supervisor.notify = mock_notify

        trade_stream_listener_thread = threading.Thread(target=trade_stream_listener.websocket_app.run_forever,
                                                        daemon=True)
        trade_stream_listener_thread.start()
        time.sleep(10)

        assert notify_called, "Notify should have been called at least once."

        trade_stream_listener.restart_websocket_app = original_restart_websocket_app
        assert trade_stream_listener.restart_websocket_app != "Mocked restart_websocket_app called", \
            "restart_websocket_app should not have been called."

        # Sprawdzenie wątków
        active_threads = [
            thread.name for thread in threading.enumerate()
            if thread is not threading.current_thread()
        ]

        presumed_thread_name = f'stream_listener blackout supervisor {StreamType.TRADE} {Market.SPOT}'

        assert trade_stream_listener._blackout_supervisor is not None, "Supervisor should be instantiated within StreamListener"
        assert isinstance(trade_stream_listener._blackout_supervisor, BlackoutSupervisor)
        assert presumed_thread_name in active_threads
        assert len(active_threads) == 2

        trade_stream_listener.websocket_app.close()
        TradeQueue.clear_instances()





'''
class TestStreamListener:

    def test_given_new_websocket_app_is_connection_established(self):

        _queue = DifferenceDepthQueue()

        stream_listener = StreamListener(queue=_queue,
                                         pairs=['BTCUSDT', 'ETHUSDT'],
                                         stream_type=StreamType.DIFFERENCE_DEPTH,
                                         market=Market.SPOT
                                         )

        _queue.currently_accepted_stream_id = stream_listener.id.id
        thread = threading.Thread(target=stream_listener.websocket_app.run_forever)
        thread.daemon = True
        thread.start()

        time.sleep(5)

        stream_listener.websocket_app.close()

        time.sleep(2)

        assert thread.is_alive() is False
        assert _queue.qsize() > 0
        assert not stream_listener.websocket_app.keep_running
        assert stream_listener.websocket_app.sock is None
'''