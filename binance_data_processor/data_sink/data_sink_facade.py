from __future__ import annotations

import pprint
import time
import multiprocessing as mp
import psutil

# from binance_data_processor.core.cli import CLI
# from binance_data_processor.core.api_srv import ApiSrv
from binance_data_processor.core.data_collector import DataCollector
from binance_data_processor.core.emergency_sender import EmergencySender
from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.core.queue_flags_pool import QueueFlagsPool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.snapshot_srv import SnapshotSrv
from binance_data_processor.core.stream_srv import StreamService
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg
from binance_data_processor.enums.save_target_enum import SaveTarget

__all__ = [
    'launch_data_sink',
    'BinanceDataSink'
]

from binance_data_processor.utils.sys import check_if_there_is_no_huge_library_imported


def launch_data_sink(cfg: DataSinkCfg) -> BinanceDataSink:
    _ = BinanceDataSink(cfg)
    _.run()
    return _

def _p_stream_srv(cfg, queue_flags_pool, stop_flag):
    proc = psutil.Process()
    proc.cpu_affinity([12])

    # print(f"STREAM: PID={proc.pid}, affinity={proc.cpu_affinity()}", flush=True)

    srv = StreamService(cfg, queue_flags_pool, stop_flag)
    srv.run()

def _p_data_collector_srv(cfg, queue_flags_pool, stop_flag) -> None:
    proc = psutil.Process()
    proc.cpu_affinity([6])

    # print(f"STREAM: PID={proc.pid}, affinity={proc.cpu_affinity()}", flush=True)

    srv = DataCollector(cfg, queue_flags_pool, stop_flag)
    srv.run()

def _p_snapshot_srv(cfg, stop_flag) -> None:
    proc = psutil.Process()
    proc.cpu_affinity([6])

    srv = SnapshotSrv(cfg, stop_flag)
    srv.run()

def _proc_emergency_sender(cfg: DataSinkCfg, stop_flag) -> None:
    proc = psutil.Process()
    proc.cpu_affinity([6])

    srv = EmergencySender(cfg, stop_flag)
    srv.start()

# def _proc_api(cfg, queue_pool) -> None:
#     proc = psutil.Process()
#     proc.cpu_affinity([6])
#
#     cli = CLI(cfg, queue_pool)
#     api = ApiSrv(cli.handle_command)
#     api.run()


class BinanceDataSink:

    __slots__ = (
        'cfg',
        'stop_flag',
        '_qfp',
        '_logger'
    )

    def __init__(self, cfg: DataSinkCfg) -> None:
        mp.set_start_method("spawn", force=True)
        self.cfg = cfg
        self.stop_flag = mp.Event()
        self._qfp = QueueFlagsPool(cfg)

        self._logger = setup_logger(dump=True)
        self._logger.info("\n%s", binance_archiver_logo)
        self._logger.info("Cfg:\n%s", pprint.pformat(cfg, indent=1))
        if cfg.save_target is SaveTarget.BACKBLAZE:
            self._logger.info(f'Bucket: {cfg.blob_conn_params.backblaze_bucket_name}\n')

    def run(self):
        p2 = mp.Process(target=_p_data_collector_srv, args=(self.cfg, self._qfp, self.stop_flag), daemon=False, name='_data_collector')
        p2.start()

        time.sleep(3)

        p1 = mp.Process(target=_p_stream_srv, args=(self.cfg, self._qfp, self.stop_flag), daemon=False, name='_stream_srv')
        p1.start()

        p3 = mp.Process(target=_p_snapshot_srv, args=(self.cfg, self.stop_flag), daemon=False, name='_snapshot_srv')
        p3.start()

        if self.cfg.save_target is SaveTarget.BACKBLAZE:
            p4 = mp.Process(target=_proc_emergency_sender, args=(self.cfg, self.stop_flag), daemon=False, name='_emergency_sender')
            p4.start()

        # p5 = mp.Process(target=_proc_api, args=(self.cfg, self._qfp), daemon=False, name='_api')
        # p5.start()

        # self._logger.info(f'{p1.name}: {p1.pid} {psutil.Process(p1.pid).cpu_affinity()}')
        # self._logger.info(f'{p2.name}: {p2.pid} {psutil.Process(p2.pid).cpu_affinity()}')
        # self._logger.info(f'{p3.name}: {p3.pid} {psutil.Process(p3.pid).cpu_affinity()}')
        # self._logger.info(f'{p4.name}: {p4.pid} {psutil.Process(p4.pid).cpu_affinity()}')
        # self._logger.info(f'{p5.name}: {p5.pid} {psutil.Process(p5.pid).cpu_affinity()}')

        try:
            check_if_there_is_no_huge_library_imported()
        except Exception as e:
            self._logger.info(f'Exception occurred: {e}')
            self.stop()

    def stop(self):
        self._logger.info("Shutting down Binance Data Archiver")

        self.stop_flag.set()

        time.sleep(5)
