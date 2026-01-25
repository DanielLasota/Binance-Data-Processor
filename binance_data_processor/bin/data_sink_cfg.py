import os
from dotenv import load_dotenv

from binance_data_processor import DataSinkCfg
from binance_data_processor import BlobConnParams

ENV_PATH = os.path.join(os.path.expanduser('~'), 'Documents/binance-archiver-ba2-v2-test.env')
if 'prod' in ENV_PATH: raise Exception('prod .env not allowed locally')
load_dotenv(ENV_PATH)


cfg = DataSinkCfg(
    instruments={
        'spot': {
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "SOLUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "ADAUSDT",
            "SHIBUSDT",
            "LTCUSDT",
            "AVAXUSDT",
            "TRXUSDT",
            "DOTUSDT",
            "BCHUSDT",
            "SUIUSDT",
            "PEPEUSDT",
            "XLMUSDT",
            "LINKUSDT",
            "VIRTUALUSDT"
        }
    },
    time_cfg={
        "file_duration_seconds": 180,
        "snapshot_fetcher_interval_seconds": 180,
        "websocket_life_time_seconds": 60*30,
    },
    data_save_target='backblaze',
    blob_conn_params=BlobConnParams(),
)
