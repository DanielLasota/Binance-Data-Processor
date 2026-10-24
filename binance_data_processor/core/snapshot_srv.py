from __future__ import annotations

import time
from abc import ABC, abstractmethod
import multiprocessing as mp

import requests

from binance_data_processor.core.data_writer import DataWriter
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.url_factory import URLFactory
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg
from binance_data_processor.utils.file_utils import get_base_of_blob_file_name
from binance_data_processor.utils.time_utils import get_utc_tz_epoch_ms
from binance_data_processor.utils.time_utils import get_utc_ts_epoch_s
from binance_data_processor.utils.time_utils import get_next_midnight_utc_epoch_s


class SnapshotStopped(Exception):
    ...

class DepthSnapshotStrategy(ABC):
    __slots__ = ()

    @abstractmethod
    def handle_snapshot(
        self,
        data: str,
        dir_: str,
        f_name: str
    ):
        ...


class DataSinkSnapshotStrategy(DepthSnapshotStrategy):
    __slots__ = (
        'data_writer',
    )

    def __init__(
        self,
        cfg: DataSinkCfg
    ):
        self.data_writer = DataWriter(cfg)

    def handle_snapshot(
        self,
        data: str,
        dir_: str,
        f_name: str
    ) -> None:
        self.data_writer.save_data(data, dir_, f_name)


class ListenerDepthSnapshotStrategy(DepthSnapshotStrategy):
    __slots__ = ['global_queue']

    def __init__(self, global_queue: mp.Queue):
        self.global_queue = global_queue

    def handle_snapshot(
        self,
        data: str,
        dir_: str,
        f_name: str
    ):
        self.global_queue.put(data)


class SnapshotSrv:
    REFRESH_INTERVAL = 4 * 3600

    __slots__ = [
        'cfg',
        'logger',
        'strategy',
        'stop_flag',
        '_session',
        '_last_refresh_time'
    ]

    def __init__(
        self,
        cfg: DataSinkCfg,
        stop_flag,
        global_queue: mp.queues.Queue | None = None
    ):
        self.cfg = cfg
        self.logger = None
        self.stop_flag = stop_flag
        self._session = requests.Session()
        self._last_refresh_time = time.time()

        if global_queue:
            self.strategy = ListenerDepthSnapshotStrategy(global_queue)
        else:
            self.strategy = DataSinkSnapshotStrategy(cfg)

    def run(self) -> None:
        self.logger = setup_logger(dump=True)
        self._snapshot_loop()

    def _snapshot_loop(self) -> None:
        next_snapshot_time = self._calc_next_snapshot_time()

        while not self.stop_flag.is_set():
            target_midnight_utc_epoch_s_plus_10s = get_next_midnight_utc_epoch_s(with_offset_seconds=10)
            current_time_s = get_utc_ts_epoch_s()
            sleep_duration = next_snapshot_time - current_time_s

            if sleep_duration < 0:
                next_snapshot_time = self._calc_next_snapshot_time()
                sleep_duration = 0

            self._sleep_with_flag_check(sleep_duration)

            while (remaining_ms := target_midnight_utc_epoch_s_plus_10s * 1000 - get_utc_tz_epoch_ms()) > 0 and remaining_ms < 1000:
                self.logger.debug('waiting 0.1s to reach midnight')
                time.sleep(0.1)

            self._fetch_and_save_snapshots()

            next_snapshot_time += self.cfg.time.snapshot_interval_seconds

            if next_snapshot_time > get_next_midnight_utc_epoch_s(with_offset_seconds=10) - 60:
                self.logger.info(f'Setting next snapshot fetch to    m i d n i g h t ')
                next_snapshot_time = get_next_midnight_utc_epoch_s(with_offset_seconds=10)

        self.logger.info("Snapshot daemon for all markets has ended")

    def _calc_next_snapshot_time(self):
        current_time_s = get_utc_ts_epoch_s()
        snapshot_fetcher_interval_seconds = self.cfg.time.snapshot_interval_seconds
        next_midnight_utc_epoch_seconds = get_next_midnight_utc_epoch_s(with_offset_seconds=10)

        next_planned_timestamp_of_fetch = current_time_s + snapshot_fetcher_interval_seconds

        if next_planned_timestamp_of_fetch > next_midnight_utc_epoch_seconds - 60:
            self.logger.info(f'Setting next snapshot fetch to    m i d n i g h t ')
            return next_midnight_utc_epoch_seconds

        return current_time_s + snapshot_fetcher_interval_seconds

    # def _fetch_and_save_snapshots(self):
    #     snapshots = []
    #
    #     for market, pairs in self.cfg.instruments.dict.items():
    #         ap = AssetParams(
    #             market=market,
    #             stream_type=StreamType.DEPTH_SNAPSHOT,
    #             pairs=pairs
    #         )
    #         for p in ap.pairs:
    #             filtered_ap = ap.get_ap_with_specified_pair(p)
    #             data = self._request_snapshot_with_timestamps(filtered_ap)
    #             f_name = get_base_of_blob_file_name(filtered_ap)
    #             snapshots.append((data, f_name))
    #
    #     for data, f_name in snapshots:
    #         try:
    #             self.strategy.handle_snapshot(data, self.cfg.save_path, f_name)
    #         except Exception as e:
    #             self.logger.error(f"Error saving snapshot data {f_name}: {e}")
    #
    #     del snapshots

    def _fetch_and_save_snapshots(self) -> None:
        snapshots = []

        for market, pairs in self.cfg.instruments.dict.items():
            for p in pairs:
                if self.stop_flag.is_set():
                    break

                ap = AssetParams(
                    market=market,
                    stream_type=StreamType.DEPTH_SNAPSHOT,
                    pairs=[p],
                )

                try:
                    data = self._request_snapshot_with_timestamps(ap)
                except SnapshotStopped:
                    break
                except (requests.RequestException, ValueError):
                    self.logger.exception(f"Did not download snapshot {market.name} {p}")
                    continue

                f_name = get_base_of_blob_file_name(ap)
                snapshots.append((data, f_name))

            if self.stop_flag.is_set():
                break

        for data, f_name in snapshots:
            try:
                self.strategy.handle_snapshot(data, self.cfg.save_path, f_name)
            except Exception:
                self.logger.exception(f"Did not save snapshot {f_name}")
                raise

    def _sleep_with_flag_check(self, duration: int) -> None:
        interval = 1
        for _ in range(0, duration, interval):
            if self.stop_flag.is_set():
                break
            time.sleep(interval)

    def _refresh_session_if_needed(self) -> None:
        current_time = time.time()
        if current_time - self._last_refresh_time > self.REFRESH_INTERVAL:
            self._session.close()
            self._session = requests.Session()
            self._last_refresh_time = current_time
            self.logger.debug("Snapshot Manager HTTP session refreshed")

    def _request_snapshot_with_timestamps(self, ap: AssetParams) -> str:
        self._refresh_session_if_needed()
        url = URLFactory.get_difference_depth_snapshot_url(ap)

        resp = None

        try:
            request_ts = get_utc_tz_epoch_ms()
            resp = self._session.get(url, timeout=10)
            recv_ts = get_utc_tz_epoch_ms()
            resp.raise_for_status()

            return f'{resp.text[:-1]},"_rq":{request_ts},"_rc":{recv_ts}}}'

        except Exception as e:
            raise Exception(f"Error fetching snapshot: {e}")
        finally:
            if resp is not None:
                resp.close()

    def stop(self) -> None:
        self.stop_flag.set()
        self._session.close()
        self.logger.info("DepthSnapshotService shut down")
