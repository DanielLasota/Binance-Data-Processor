from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from queue import Queue

import requests

from binance_archiver import DataSinkConfig
from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.stream_data_save_and_sender import StreamDataSaverAndSender
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.timestamps_generator import TimestampsGenerator
from binance_archiver.url_factory import URLFactory


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

    __slots__ = [
        'data_saver',
        'data_save_target'
    ]

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

    __slots__ = [
        'logger',
        'snapshot_strategy',
        'data_sink_config',
        'global_shutdown_flag'
    ]

    def __init__(
        self,
        snapshot_strategy: DepthSnapshotStrategy,
        data_sink_config: DataSinkConfig,
        global_shutdown_flag: threading.Event
    ):
        self.logger = logging.getLogger('binance_archiver')
        self.snapshot_strategy = snapshot_strategy
        self.data_sink_config = data_sink_config
        self.global_shutdown_flag = global_shutdown_flag

    def run(self) -> None:
        for market, pairs in self.data_sink_config.instruments.dict.items():
            asset_parameters = AssetParameters(
                market=market,
                stream_type=StreamType.DEPTH_SNAPSHOT,
                pairs=pairs
            )
            self.start_snapshot_daemon(asset_parameters=asset_parameters)

    def start_snapshot_daemon(
        self,
        asset_parameters: AssetParameters
    ) -> None:
        thread = threading.Thread(
            target=self._snapshot_daemon,
            args=[asset_parameters],
            name=f'snapshot_daemon: market: {asset_parameters.market}'
        )
        thread.start()

    def _snapshot_daemon(
        self,
        asset_parameters: AssetParameters
    ) -> None:
        while not self.global_shutdown_flag.is_set():
            for pair in asset_parameters.pairs:
                try:
                    message = self._request_snapshot_with_timestamps(asset_parameters=asset_parameters)

                    file_name = StreamDataSaverAndSender.get_file_name(
                        asset_parameters=asset_parameters.get_asset_parameter_with_specified_pair(
                            pair=pair
                        )
                    )

                    self.snapshot_strategy.handle_snapshot(
                        json_content=message,
                        file_name=file_name,
                        file_save_catalog=self.data_sink_config.file_save_catalog
                    )

                except Exception as e:
                    self.logger.error(f"Error whilst fetching snapshot: {pair} {asset_parameters.market}: {e}")

            self._sleep_with_flag_check(self.data_sink_config.time_settings.snapshot_fetcher_interval_seconds)

        self.logger.info(f"{asset_parameters.market}: snapshot daemon has ended")

    def _sleep_with_flag_check(
            self,
            duration: int
    ) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.global_shutdown_flag.is_set():
                break
            time.sleep(interval)

    @staticmethod
    def _request_snapshot_with_timestamps(asset_parameters: AssetParameters) -> str:
        url = URLFactory.get_difference_depth_snapshot_url(asset_parameters)

        try:
            request_timestamp = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            response = requests.get(url, timeout=5)
            receive_timestamp = TimestampsGenerator.get_utc_timestamp_epoch_milliseconds()
            response.raise_for_status()

            message = (response.text[:-1]
                       + f',"_rq":{request_timestamp}'
                         f',"_rc":{receive_timestamp}'
                         f'}}'
                       )

            return message

        except Exception as e:
            raise Exception(f"Error whilst fetching snapshot: {e}")
