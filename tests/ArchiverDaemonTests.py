import re
from datetime import datetime, timezone

from binance_archiver.orderbook_level_2_listener.ArchiverDaemon import ArchiverDaemon


class TestArchiverDaemon:

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

    def test_timestamps_are_in_utc(self):
        timestamp_seconds_method = ArchiverDaemon._get_utc_timestamp_epoch_seconds()
        timestamp_milliseconds_method = ArchiverDaemon._get_utc_timestamp_epoch_milliseconds()

        datetime_seconds = datetime.fromtimestamp(timestamp_seconds_method, tz=timezone.utc)
        datetime_milliseconds = datetime.fromtimestamp(timestamp_milliseconds_method / 1000, tz=timezone.utc)

        assert datetime_seconds.tzinfo == timezone.utc, "The timestamp in seconds is not in UTC."
        assert datetime_milliseconds.tzinfo == timezone.utc, "The timestamp in milliseconds is not in UTC."
