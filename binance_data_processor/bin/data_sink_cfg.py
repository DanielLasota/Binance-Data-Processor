import os
from dotenv import load_dotenv

from binance_data_processor import DataSinkCfg
from binance_data_processor.enums.instruments_matrix import InstrumentsMatrix
from binance_data_processor.enums.interval_settings import TimeCfg

ENV_PATH = os.path.join(os.path.expanduser('~'), 'Documents/binance-archiver-ba2-v2-test.env')
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
    time_cfg=TimeCfg(
        file_duration_seconds=180,
        snapshot_fetcher_interval_seconds=180,
        websocket_lifetime_seconds=60*30,
    ),
    data_save_target='zip'
)
