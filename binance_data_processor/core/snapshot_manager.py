from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from queue import Queue
import requests

from binance_data_processor.core.stream_data_saver_and_sender import StreamDataSaverAndSender
from binance_data_processor.core.url_factory import URLFactory
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.enums.data_sink_config import DataSinkConfig
from binance_data_processor.utils.file_utils import get_base_of_blob_file_name
from binance_data_processor.utils.time_utils import (
    get_utc_timestamp_epoch_milliseconds,
    get_utc_timestamp_epoch_seconds,
    get_next_midnight_utc_epoch_seconds
)


class DepthSnapshotStrategy(ABC):
    __slots__ = ()

    @abstractmethod
    def handle_snapshot(
        self,
        json_content: str,
        file_save_catalog: str,
        file_name: str
    ):
        ...


class DataSinkDepthSnapshotStrategy(DepthSnapshotStrategy):
    __slots__ = ['data_saver']

    def __init__(
        self,
        data_saver: StreamDataSaverAndSender
    ):
        self.data_saver = data_saver

    def handle_snapshot(
        self,
        json_content: str,
        file_save_catalog: str,
        file_name: str
    ) -> None:
        self.data_saver.save_data(
            json_content=json_content,
            file_save_catalog=file_save_catalog,
            file_name=file_name
        )


class ListenerDepthSnapshotStrategy(DepthSnapshotStrategy):
    __slots__ = ['global_queue']

    def __init__(self, global_queue: Queue):
        self.global_queue = global_queue

    def handle_snapshot(
        self,
        json_content: str,
        file_save_catalog: str,
        file_name: str
    ):
        self.global_queue.put(json_content)


class DepthSnapshotService:
    REFRESH_INTERVAL = 4 * 3600

    __slots__ = [
        'logger',
        'snapshot_strategy',
        'data_sink_config',
        'global_shutdown_flag',
        '_session',
        '_last_refresh_time',
        '_thread'
    ]

    def __init__(
        self,
        snapshot_strategy: DepthSnapshotStrategy,
        data_sink_config: DataSinkConfig,
        global_shutdown_flag: threading.Event
    ):
        self.logger = logging.getLogger('binance_data_sink')
        self.snapshot_strategy = snapshot_strategy
        self.data_sink_config = data_sink_config
        self.global_shutdown_flag = global_shutdown_flag
        self._session = requests.Session()
        self._last_refresh_time = time.time()
        self._thread = None

    def run(self) -> None:
        self._thread = threading.Thread(
            target=self._snapshot_daemon,
            name='snapshot_daemon_all_markets'
        )
        self._thread.start()

    def _snapshot_daemon(self) -> None:
        next_snapshot_time = self._calculate_next_snapshot_time()

        while not self.global_shutdown_flag.is_set():
            target_midnight_utc_epoch_s_plus_10s = get_next_midnight_utc_epoch_seconds(with_offset_seconds=10)
            current_time_s = get_utc_timestamp_epoch_seconds()
            sleep_duration = next_snapshot_time - current_time_s

            if sleep_duration < 0:
                next_snapshot_time = self._calculate_next_snapshot_time()
                sleep_duration = 0

            self._sleep_with_flag_check(sleep_duration)

            while (remaining_ms := target_midnight_utc_epoch_s_plus_10s * 1000 - get_utc_timestamp_epoch_milliseconds()) > 0 and remaining_ms < 1000:
                self.logger.debug('waiting 0.1s to reach midnight')
                time.sleep(0.1)

            self._fetch_and_save_snapshots()

            next_snapshot_time += self.data_sink_config.time_settings.snapshot_fetcher_interval_seconds

            if next_snapshot_time > get_next_midnight_utc_epoch_seconds(with_offset_seconds=10) - 60:
                self.logger.info(f'Setting next snapshot fetch to    m i d n i g h t ')
                next_snapshot_time = get_next_midnight_utc_epoch_seconds(with_offset_seconds=10)

        self.logger.info("Snapshot daemon for all markets has ended")

    def _calculate_next_snapshot_time(self):
        current_time_s = get_utc_timestamp_epoch_seconds()
        snapshot_fetcher_interval_seconds = self.data_sink_config.time_settings.snapshot_fetcher_interval_seconds
        next_midnight_utc_epoch_seconds = get_next_midnight_utc_epoch_seconds(with_offset_seconds=10)

        next_planned_timestamp_of_fetch = current_time_s + snapshot_fetcher_interval_seconds

        if next_planned_timestamp_of_fetch > next_midnight_utc_epoch_seconds - 60:
            self.logger.info(f'Setting next snapshot fetch to    m i d n i g h t ')
            return next_midnight_utc_epoch_seconds

        return current_time_s + snapshot_fetcher_interval_seconds

    def _fetch_and_save_snapshots(self):
        all_snapshots = []

        for market, pairs in self.data_sink_config.instruments.dict.items():
            asset_parameters = AssetParameters(
                market=market,
                stream_type=StreamType.DEPTH_SNAPSHOT,
                pairs=pairs
            )
            for pair in asset_parameters.pairs:
                try:
                    snapshot = self._request_snapshot_with_timestamps(
                        asset_parameters=asset_parameters.get_asset_parameter_with_specified_pair(pair=pair)
                    )
                    file_name = get_base_of_blob_file_name(
                        asset_parameters=asset_parameters.get_asset_parameter_with_specified_pair(pair=pair)
                    )
                    all_snapshots.append((snapshot, file_name))
                except Exception as e:
                    self.logger.error(f"Error fetching snapshot {pair} {market}: {e}")

        for snapshot, file_name in all_snapshots:
            try:
                self.snapshot_strategy.handle_snapshot(
                    json_content=snapshot,
                    file_name=file_name,
                    file_save_catalog=self.data_sink_config.file_save_catalog
                )
            except Exception as e:
                self.logger.error(f"Error saving snapshot {file_name}: {e}")

        del all_snapshots

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    def _refresh_session_if_needed(self) -> None:
        current_time = time.time()
        if current_time - self._last_refresh_time > self.REFRESH_INTERVAL:
            self._session.close()
            self._session = requests.Session()
            self._last_refresh_time = current_time
            self.logger.debug("Snapshot Manager HTTP session refreshed")

    def _request_snapshot_with_timestamps(self, asset_parameters: AssetParameters) -> str:
        self._refresh_session_if_needed()
        url = URLFactory.get_difference_depth_snapshot_url(asset_parameters)

        response = None

        try:
            request_timestamp = get_utc_timestamp_epoch_milliseconds()
            response = self._session.get(url, timeout=10)
            receive_timestamp = get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()

            message = (
                    response.text[:-1]
                    + f',"_rq":{request_timestamp},'
                      f'"_rc":{receive_timestamp}}}'
            )
            return message

        except Exception as e:
            raise Exception(f"Error fetching snapshot: {e}")
        finally:
            if response is not None:
                response.close()

    def shutdown(self) -> None:
        self.global_shutdown_flag.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._session.close()
        self.logger.info("DepthSnapshotService shut down")
