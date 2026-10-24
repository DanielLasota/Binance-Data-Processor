from datetime import datetime, timedelta
import time

from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.time_unit import TimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


def get_utc_ts_epoch_s() -> int:
    raw_timestamp_of_receive_ns = time.time_ns()
    return (raw_timestamp_of_receive_ns + 500_000_000) // 1_000_000_000

def get_utc_tz_epoch_ms() -> int:
    raw_timestamp_of_receive_ns = time.time_ns()
    return (raw_timestamp_of_receive_ns + 500_000) // 1_000_000

def get_utc_ts_epoch_us() -> int:
    raw_timestamp_of_receive_ns = time.time_ns()
    return (raw_timestamp_of_receive_ns + 500) // 1_000

def round_epoch_ns_to_ms(ns: int) -> int:
    return (ns + 500_000) // 1_000_000

def round_epoch_ns_to_us(ns: int) -> int:
    return (ns + 500) // 1_000

def get_next_midnight_utc_epoch_s(with_offset_seconds: int = 0) -> int:
    current_time_s = get_utc_ts_epoch_s()

    seconds_since_midnight = current_time_s % (24 * 3600)
    seconds_to_midnight = (24 * 3600) - seconds_since_midnight

    return current_time_s + seconds_to_midnight + with_offset_seconds

def get_utc_formatted_ts_for_file_name() -> str:
    return datetime.utcnow().strftime("%d-%m-%YT%H-%M-%SZ")

def gen_dates(date_range: list[str]) -> list[str]:
    date_format = "%d-%m-%Y"
    start_date_str, end_date_str = date_range

    try:
        start_date = datetime.strptime(start_date_str, date_format)
        end_date = datetime.strptime(end_date_str, date_format)
    except ValueError as ve:
        raise ValueError(f"invalid date format{ve}")

    if start_date > end_date:
        raise ValueError("start date > end_date")

    date_list = []

    delta = end_date - start_date

    for i in range(delta.days + 1):
        current_date = start_date + timedelta(days=i)
        date_str = current_date.strftime(date_format)
        date_list.append(date_str)

    return date_list

def get_yesterday_date(date: str) -> str:
    date = datetime.strptime(date, "%d-%m-%Y")
    yesterday_date = date - timedelta(days=1)
    return yesterday_date.strftime("%d-%m-%Y")

def resolve_epoch_time_unit(ap: AssetParams) -> TimeUnit:
    if ap.market is Market.SPOT and ap.stream_type is not StreamType.DEPTH_SNAPSHOT:
        return TimeUnit.US

    return TimeUnit.MS
