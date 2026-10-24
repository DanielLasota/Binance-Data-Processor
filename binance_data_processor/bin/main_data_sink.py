from binance_data_processor import launch_data_sink
from data_sink_cfg import cfg

import time
import multiprocessing as mp


if __name__ == "__main__":

    mp.set_start_method("spawn", force=True)

    ds = launch_data_sink(cfg)

    while not ds.stop_flag.is_set():
        time.sleep(10)
