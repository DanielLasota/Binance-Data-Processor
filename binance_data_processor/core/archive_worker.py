import json
import time
from collections import defaultdict

from binance_data_processor.core.data_writer import DataWriter
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.utils.file_utils import get_base_of_blob_file_name


class DataArchiver:
    def __init__(self, cfg, srb):
        self.cfg = cfg
        self.srb = srb
        self._data_writer = DataWriter(cfg)
        self._buffers = defaultdict(list)

    def run(self):
        deadline = time.monotonic() + self.cfg.time.file_duration_seconds

        try:
            while True:
                item = self.srb.get_nowait()

                if item is not None:
                    _, _, payload = item

                    if payload == "":
                        break

                    self._collect(payload)

                if time.monotonic() >= deadline:
                    self._flush()
                    deadline = (time.monotonic() + self.cfg.time.file_duration_seconds)

                if item is None:
                    time.sleep(0.001)

            self._flush()
        finally:
            try:
                self._data_writer.stop()
            finally:
                self.srb.close()

    def _collect(self, payload):
        market, stream_type, msg = payload.split("\n", 2)
        pair = json.loads(msg)["stream"].split("@", 1)[0]
        self._buffers[(market, stream_type, pair)].append(msg)

    def _flush(self):
        for key, messages in list(self._buffers.items()):
            market, stream_type, pair = key

            ap = AssetParams(market=Market[market], stream_type=StreamType[stream_type], pairs=[pair])
            f_name = get_base_of_blob_file_name(ap)
            json_payload = "[" + ",".join(messages) + "]"

            self._data_writer.save_data(json_payload, self.cfg.save_path, f_name)
            del self._buffers[key]


def run_archive_worker(cfg, srb):
    DataArchiver(cfg, srb).run()
