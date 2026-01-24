from dataclasses import dataclass, field
import os

from binance_data_processor.enums.data_save_target_enum import DataSaveTarget
from binance_data_processor.enums.instruments_matrix import InstrumentsMatrix
from binance_data_processor.enums.interval_settings import TimeCfg
from binance_data_processor.enums.storage_connection_parameters import BlobConnParams


@dataclass(slots=True)
class DataSinkCfg:
    instruments: InstrumentsMatrix | dict[str, any] = field(
        default_factory=lambda: InstrumentsMatrix(
            spot=['BTCUSDT'],
            usd_m_futures=['BTCUSDT'],
            coin_m_futures=['BTCUSDT_PERP']
        )
    )
    time_cfg: TimeCfg | dict[str, any] = field(
        default_factory=lambda: TimeCfg(
            file_duration_seconds=300,
            snapshot_fetcher_interval_seconds=180,
            websocket_lifetime_seconds=60 * 30
        )
    )
    data_save_target: DataSaveTarget | str = DataSaveTarget.JSON
    blob_conn_params: BlobConnParams | dict[str, str] | None = field(default=None, repr=False)
    file_save_catalog: str = '../dump/'
    show_logo: bool = True

    def validate(self):
        if not isinstance(self.data_save_target, DataSaveTarget):
            raise ValueError("Invalid data_save_target value.")

        if self.file_save_catalog:
            catalog_path = os.path.abspath(self.file_save_catalog)
            if not os.path.exists(catalog_path):
                print(f"Directory '{catalog_path}' does not exist. Creating it...")
                try:
                    os.makedirs(catalog_path, exist_ok=True)
                    print(f"Directory '{catalog_path}' created successfully.")
                except OSError as e:
                    raise ValueError(f"Cannot create directory '{catalog_path}': {e}")

        if not self.blob_conn_params:
            raise ValueError("Storage connection parameters must be provided.")

        if not isinstance(self.time_cfg, TimeCfg):
            raise ValueError("time_settings must be an instance of IntervalSettings.")

    def __post_init__(self):
        if isinstance(self.instruments, dict):
            instruments_kwargs = {}
            if 'spot' in self.instruments and self.instruments['spot']:
                instruments_kwargs['spot'] = self.instruments['spot']
            if 'usd_m_futures' in self.instruments and self.instruments['usd_m_futures']:
                instruments_kwargs['usd_m_futures'] = self.instruments['usd_m_futures']
            if 'coin_m_futures' in self.instruments and self.instruments['coin_m_futures']:
                instruments_kwargs['coin_m_futures'] = self.instruments['coin_m_futures']
            self.instruments = InstrumentsMatrix(**instruments_kwargs)

        if isinstance(self.blob_conn_params, dict):
            self.blob_conn_params = BlobConnParams(**self.blob_conn_params)
        elif self.blob_conn_params is None:
            self.blob_conn_params = BlobConnParams()

        if isinstance(self.data_save_target, str):
            try:
                self.data_save_target = DataSaveTarget(self.data_save_target.lower())
            except ValueError:
                raise ValueError(f"Invalid data_save_target value: {self.data_save_target}")

        if isinstance(self.time_cfg, dict):
            self.time_cfg = TimeCfg(
                file_duration_seconds=self.time_cfg.get('file_duration_seconds', 0),
                snapshot_fetcher_interval_seconds=self.time_cfg.get('snapshot_fetcher_interval_seconds', 0),
                websocket_lifetime_seconds=self.time_cfg.get('websocket_life_time_seconds', 0)
            )

        self.validate()
