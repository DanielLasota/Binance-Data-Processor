import re
from datetime import datetime, timezone

import pytest

from binance_archiver.orderbook_level_2_listener.archiver_daemon import ArchiverDaemon, BadConfigException, launch_data_sink, BadAzureParameters


class TestArchiverDaemon:

    def test_given_config_has_no_instrument_then_is_exception_thrown(self):

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

    def test_given_market_type_is_empty_then_is_exception_thrown(
        self
    ):
        config = {
            "instruments": {
                "spot": [],  # Empty market type
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

    def test_given_too_many_markets_then_is_exception_thrown(
        self
    ):
        config = {
            "instruments": {
                "spot": ["BTCUSDT", "ETHUSDT"],
                "usd_m_futures": ["BTCUSDT", "ETHUSDT"],
                "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"],
                "actions": ["AAPL", "TSLA"],  # Extra market
                "mining": ["BTCMINING"]  # Extra market
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

    def test_given_not_handled_market_type_then_is_exception_thrown(
        self
    ):
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

    def test_given_send_zip_to_blob_is_true_and_azure_blob_parameters_with_key_is_bad_then_is_exception_thrown(self):
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
            launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
            )

        assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

    def test_given_send_zip_to_blob_is_true_and_container_name_is_bad_then_is_exception_thrown(self):
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
            launch_data_sink(
                    config=config,
                    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
                    container_name=container_name
            )

        assert str(excinfo.value) == "Azure blob parameters with key or container name is missing or empty"

    def test_get_utc_formatted_timestamp(self):
        timestamp = ArchiverDaemon._get_utc_formatted_timestamp()
        pattern = re.compile(r'\d{2}-\d{2}-\d{4}T\d{2}-\d{2}-\d{2}Z')
        assert pattern.match(timestamp), f"Timestamp {timestamp} does not match the expected format %d-%m-%YT%H-%M-%SZ"

    def test_get_utc_timestamp_epoch_seconds(self):
        timestamp_seconds_method = ArchiverDaemon._get_utc_timestamp_epoch_seconds()
        timestamp_seconds_now = round(datetime.now(timezone.utc).timestamp())

        assert (abs(timestamp_seconds_method - timestamp_seconds_now) < 2,
                "The timestamp in seconds is not accurate or not in UTC.")

    def test_get_utc_timestamp_epoch_milliseconds(self):
        timestamp_milliseconds_method = ArchiverDaemon._get_utc_timestamp_epoch_milliseconds()
        timestamp_milliseconds_now = round(datetime.now(timezone.utc).timestamp() * 1000)

        assert (abs(timestamp_milliseconds_method - timestamp_milliseconds_now) < 2000,
                "The timestamp in milliseconds is not accurate or not in UTC.")

    def test_given_get_actual_epoch_timestamp_are_timestamps_in_utc(self):
        timestamp_seconds_method = ArchiverDaemon._get_utc_timestamp_epoch_seconds()
        timestamp_milliseconds_method = ArchiverDaemon._get_utc_timestamp_epoch_milliseconds()

        datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
        datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

        assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
        assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."
