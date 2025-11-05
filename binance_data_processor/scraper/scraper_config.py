from dataclasses import dataclass

from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.utils.file_utils import prepare_dump_path_catalog
from binance_data_processor.utils.time_utils import generate_dates_string_list_from_range


AMOUNT_OF_FILES_TO_BE_DOWNLOADED_AT_ONCE = 10


@dataclass
class DataScraperConfig:
    date_range: list[str]
    pairs: list[str]
    markets: list[str | Market]
    stream_types: list[str | StreamType]
    dump_path: str
    skip_existing: bool = True
    verbose: bool = False

    def __post_init__(self):
        self.dates_to_be_downloaded = generate_dates_string_list_from_range(self.date_range)

        prepare_dump_path_catalog(self.dump_path)

        if isinstance(self.markets[0], str):
            self.markets = [Market(market.lower()) for market in self.markets]
        if isinstance(self.stream_types[0], str):
            self.stream_types = [StreamType(stream_type.lower()) for stream_type in self.stream_types]
