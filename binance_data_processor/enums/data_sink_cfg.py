from dataclasses import dataclass, field
import os

from binance_data_processor.enums.save_target_enum import SaveTarget
from binance_data_processor.enums.instruments_matrix import InstrumentsMatrix
from binance_data_processor.enums.time_cfg import TimeCfg
from binance_data_processor.enums.blob_conn_params import BlobConnParams


@dataclass(slots=True)
class DataSinkCfg:
    instruments: InstrumentsMatrix
    time: TimeCfg
    save_target: SaveTarget | str
    blob_conn_params: BlobConnParams | None = field(default=None, repr=False)
    save_path: str = '../dump/'
    show_logo: bool = True

    def __post_init__(self):
        if self.save_path:
            path_ = os.path.abspath(self.save_path)
            if not os.path.exists(path_):
                os.makedirs(path_, exist_ok=True)
                print(f"Directory '{path_}' created successfully.")
