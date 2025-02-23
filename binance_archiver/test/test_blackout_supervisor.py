import pytest
import time
from unittest.mock import MagicMock

from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.blackout_supervisor import BlackoutSupervisor


@pytest.fixture
def supervisor_fixture():
    return BlackoutSupervisor(
        asset_parameters=AssetParameters(market=Market.SPOT, stream_type=StreamType.DIFFERENCE_DEPTH_STREAM, pairs=[]),
        max_interval_without_messages_in_seconds=2,
    )

class TestBlackoutSupervisor:

    def test_given_blackout_supervisor_when_initialized_then_has_correct_parameters(self):
        stream_type = StreamType.DIFFERENCE_DEPTH_STREAM
        market = Market.USD_M_FUTURES
        max_interval = 10

        supervisor = BlackoutSupervisor(
            asset_parameters=AssetParameters(
                market=market,
                stream_type=stream_type,
                pairs=[]
            ),
            max_interval_without_messages_in_seconds=max_interval,
        )

        assert supervisor.asset_parameters.stream_type == stream_type
        assert supervisor.asset_parameters.market == market
        assert supervisor.max_interval_without_messages_in_seconds == max_interval

    def test_given_blackout_supervisor_when_notify_then_updates_last_message_time(self, supervisor_fixture):
        initial_time = supervisor_fixture._last_message_time_epoch_seconds_utc
        time.sleep(1)

        supervisor_fixture.notify()

        assert supervisor_fixture._last_message_time_epoch_seconds_utc > initial_time

    def test_given_blackout_supervisor_when_no_messages_for_max_interval_then_logger_warns(self, supervisor_fixture):
        mock_on_error_callback = MagicMock()
        supervisor_fixture.on_error_callback = mock_on_error_callback
        supervisor_fixture.run()

        time.sleep(6)

        mock_on_error_callback.assert_called_once()

        supervisor_fixture.shutdown_supervisor()

    def test_given_blackout_supervisor_when_running_then_notifies_correctly(self, supervisor_fixture):
        supervisor_fixture.run()

        time.sleep(1)

        assert supervisor_fixture._running

        supervisor_fixture.shutdown_supervisor()

    def test_given_blackout_supervisor_when_shutdown_then_stops_running(self, supervisor_fixture):
        supervisor_fixture.run()

        supervisor_fixture.shutdown_supervisor()

        assert not supervisor_fixture._running

    def test_given_blackout_supervisor_when_no_error_callback_then_raises_exception(self, supervisor_fixture):
        ...
