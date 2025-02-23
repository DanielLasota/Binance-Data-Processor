from dataclasses import dataclass, field
import os

from binance_archiver.enum_.data_save_target_enum import DataSaveTarget
from binance_archiver.enum_.instruments_matrix import InstrumentsMatrix
from binance_archiver.enum_.storage_connection_parameters import (
    StorageConnectionParameters,
    load_storage_connection_parameters_from_environ
)
from binance_archiver.enum_.interval_settings import IntervalSettings


@dataclass(slots=True)
class DataSinkConfig:
    instruments: InstrumentsMatrix | dict[str, any] = field(
        default_factory=lambda: InstrumentsMatrix(
            spot=['BTCUSDT'],
            usd_m_futures=['BTCUSDT'],
            coin_m_futures=['BTCUSDT_PERP']
        )
    )
    time_settings: IntervalSettings | dict[str, any] = field(
        default_factory=lambda: IntervalSettings(
            file_duration_seconds=120,
            snapshot_fetcher_interval_seconds=300,
            websocket_life_time_seconds=60*60*23
        )
    )
    data_save_target: DataSaveTarget | str = DataSaveTarget.JSON
    storage_connection_parameters: StorageConnectionParameters | dict[str, str] | None = field(
        default=None,
        repr=False
    )
    file_save_catalog: str = 'dump/'

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

        if not self.storage_connection_parameters:
            raise ValueError("Storage connection parameters must be provided.")

        if not isinstance(self.time_settings, IntervalSettings):
            raise ValueError("time_settings must be an instance of TimeSettings.")

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

        if isinstance(self.storage_connection_parameters, dict):
            self.storage_connection_parameters = StorageConnectionParameters(**self.storage_connection_parameters)
        elif self.storage_connection_parameters is None:
            self.storage_connection_parameters = load_storage_connection_parameters_from_environ()

        if isinstance(self.data_save_target, str):
            try:
                self.data_save_target = DataSaveTarget(self.data_save_target.lower())
            except ValueError:
                raise ValueError(f"Invalid data_save_target value: {self.data_save_target}")

        if isinstance(self.time_settings, dict):
            self.time_settings = IntervalSettings(
                file_duration_seconds=self.time_settings.get('file_duration_seconds', 0),
                snapshot_fetcher_interval_seconds=self.time_settings.get('snapshot_fetcher_interval_seconds', 0),
                websocket_life_time_seconds=self.time_settings.get('websocket_life_time_seconds', 0)
            )

        self.validate()
