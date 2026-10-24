from __future__ import annotations

import re
import time
import multiprocessing as mp
from uuid import uuid4

from binance_data_processor.core.queue_flags_pool import QueueFlagsPool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.core.shared_ring_buffer import SharedRingBuffer
from binance_data_processor.core.queue_pool import DataSinkQueuePool
from binance_data_processor.core.archive_worker import run_archive_worker
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg


class DataCollector:

    __slots__ = (
        'cfg',
        'queue_flags_pool',
        'stop_flag',
        'logger',
        '_queue_pool',
        '_stream_message_pair_pattern',
        "_collector_to_archiver_srb",
        "_arch_process",
    )

    _STREAM_MSG_PATTERN = re.compile(r'"stream":"(\w+)@')
    _SRB_BURST = 64
    _ARCH_CAPACITY = 1 << 24 #16 MiB

    def __init__(
        self,
        cfg: DataSinkCfg,
        queue_flags_pool: QueueFlagsPool,
        stop_flag,
    ):

        self.cfg = cfg
        self.queue_flags_pool: QueueFlagsPool = queue_flags_pool
        self.stop_flag = stop_flag

        self.logger = None
        self._queue_pool = DataSinkQueuePool(self.cfg)

        self._collector_to_archiver_srb = None
        self._arch_process = None

    # def run(self):
    #     self.logger = setup_logger(dump=True)
    #
    #     self._arch_srb = SharedRingBuffer.create(f"archive_{uuid4().hex}", self._ARCH_CAPACITY, )
    #     self._arch_process = mp.Process(target=run_archive_worker, args=(self.cfg, self._arch_srb),)
    #
    #     try:
    #         self._arch_process.start()
    #
    #         while not self.stop_flag.is_set():
    #             self._check_archiver()
    #
    #             processed = self._drain_client_srb()
    #             self._forward_to_arch_srb()
    #
    #             if processed == 0:
    #                 self.stop_flag.wait(0.001)
    #
    #         while self._drain_client_srb():
    #             self._forward_to_arch_srb()
    #
    #         self._forward_to_arch_srb()
    #
    #     finally:
    #         self._stop_archiver()

    def run(self):
        self.logger = setup_logger(dump=True)

        self._collector_to_archiver_srb = SharedRingBuffer.create(
            f"collector_to_archiver_{uuid4().hex}",
            self._ARCH_CAPACITY,
        )
        self._arch_process = None

        try:
            self._arch_process = mp.Process(
                target=run_archive_worker,
                args=(self.cfg, self._collector_to_archiver_srb),
            )
            self._arch_process.start()

            stop_requested = self.stop_flag.is_set
            drain = self._drain_client_srb
            forward = self._forward_to_arch_srb
            check_archiver = self._check_archiver
            monotonic = time.monotonic

            next_health_check = 0.0

            while not stop_requested():
                now = monotonic()

                if now >= next_health_check:
                    check_archiver()
                    next_health_check = now + 0.1

                drain()
                forward()

                # Busy-spin: immediate recheck queues
                # No sleep n wait even without data

            while drain():
                forward()

            forward()

        finally:
            if self._arch_process is not None:
                self._stop_archiver()
            else:
                try:
                    self._collector_to_archiver_srb.close()
                finally:
                    self._collector_to_archiver_srb.unlink()

    def _drain_client_srb(self) -> int:
        processed = 0

        for queue in self._queue_pool.queues.values():
            for _ in range(self._SRB_BURST):
                item = queue.srb.get_nowait()

                if item is None:
                    break

                cid, ts, msg = item

                queue.put(msg, ts, bool(cid),)

                processed += 1

        return processed

    def _check_archiver(self):
        if not self._arch_process.is_alive():
            raise RuntimeError("Proces zapisujący zakończył pracę")

    def _send(self, payload):
        if len(payload.encode("utf-8")) + 13 > self._ARCH_CAPACITY:
            raise ValueError("Wiadomość jest większa niż archiwizacyjny SRB")

        while True:
            self._check_archiver()

            if self._collector_to_archiver_srb.put_nowait(payload, ts=0, cid=0):
                return

            time.sleep(0.1)

    def _forward_to_arch_srb(self):
        for (market, stream_type), queue in self._queue_pool.queues.items():
            while queue.queue:
                msg = queue.queue[0]
                payload = f"{market.name}\n{stream_type.name}\n{msg}"

                self._send(payload)

                queue.get_nowait()

    def _stop_archiver(self):
        try:
            if self._arch_process.pid is not None:
                if self._arch_process.is_alive():
                    self._send("")  # Koniec danych.

                self._arch_process.join()

                if self._arch_process.exitcode != 0:
                    raise RuntimeError(f"Proces zapisujący zakończył się błędem: {self._arch_process.exitcode}")
        finally:
            self._collector_to_archiver_srb.close()
            self._collector_to_archiver_srb.unlink()
