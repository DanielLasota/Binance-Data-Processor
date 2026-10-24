import os
from dotenv import load_dotenv

from binance_data_processor import DataSinkCfg, BlobConnParams
from binance_data_processor.enums.save_target_enum import SaveTarget
from binance_data_processor.enums.instruments_matrix import InstrumentsMatrix
from binance_data_processor.enums.time_cfg import TimeCfg

ENV_PATH = os.path.join(os.path.expanduser('~'), 'Documents/env/binance-archiver-ba2-v2-test.env')
if 'prod' in ENV_PATH: raise Exception('prod .env not allowed locally')
load_dotenv(ENV_PATH)


cfg = DataSinkCfg(
    instruments=InstrumentsMatrix(
        spot=[
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
        ],
        usd_m_futures=[
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "SOLUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "ADAUSDT",
            "LTCUSDT",
            "AVAXUSDT",
            "TRXUSDT",
            "DOTUSDT",
            "BCHUSDT",
            "SUIUSDT",
            "1000PEPEUSDT",
            "XLMUSDT",
            "LINKUSDT",
            "HYPEUSDT",
            "1000SHIBUSDT",
            "VIRTUALUSDT"
        ],
        coin_m_futures=[
            "BTCUSD_PERP",
            "ETHUSD_PERP",
            "BNBUSD_PERP",
            "SOLUSD_PERP",
            "XRPUSD_PERP",
            "DOGEUSD_PERP",
            "ADAUSD_PERP",
            "LTCUSD_PERP",
            "AVAXUSD_PERP",
            "TRXUSD_PERP",
            "DOTUSD_PERP",
            "BCHUSD_PERP",
            "SUIUSD_PERP",
            "LINKUSD_PERP",
            "XLMUSD_PERP"
        ]
    ),
    time=TimeCfg(
        file_duration_seconds=180,
        snapshot_interval_seconds=180,
        # ws_lifetime_seconds=60*30,
        ws_lifetime_seconds=30,
    ),
    save_target=SaveTarget.JSON,
    blob_conn_params=BlobConnParams()
)
