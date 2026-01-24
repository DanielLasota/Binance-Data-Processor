import time
from binance_data_processor import launch_data_sink
from data_sink_cfg import cfg


if __name__ == "__main__":

    ds = launch_data_sink(cfg)

    while not ds.stop_flag.is_set():
        time.sleep(16)
