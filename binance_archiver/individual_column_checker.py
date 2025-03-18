from __future__ import annotations

import pandas as pd

from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.enum_.epoch_time_unit import EpochTimeUnit
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType


class IndividualColumnChecker:
    __slots__ = ()

    @staticmethod
    def is_there_only_one_unique_value_in_series(series: pd.Series) -> bool:
        return len(series.unique()) == 1

    @staticmethod
    def is_whole_series_made_of_only_one_expected_value(series: pd.Series, expected_value: any) -> bool:
        return series.unique()[0] == expected_value and len(series.unique()) == 1

    @staticmethod
    def is_whole_series_made_of_set_of_expected_values(series: pd.Series, expected_values: set[any]) -> bool:
        return set(series.unique()) <= expected_values

    @staticmethod
    def is_series_non_decreasing(series: pd.Series) -> bool:
        return series.diff().min() >= 0

    @staticmethod
    def is_whole_series_epoch_valid(series: pd.Series) -> bool:
        return (
                series.notna().all()
                and series.gt(0).all()
                and series.astype(float).eq(series.astype(int)).all()
                and series.dtype == int
        )

    @staticmethod
    def are_all_within_utc_z_day_range(series: pd.Series, date: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)

        day_length = 86_400 * epoch_time_unit.multiplier_of_second

        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1

        return series.between(day_start_ms, day_end_ms).all()

    @staticmethod
    def is_receive_time_column_close_to_event_time_column_by_minus_100_ms_plus_5_s(receive_time_column: pd.Series, event_time_column: pd.Series, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        five_seconds = 1 * epoch_time_unit.multiplier_of_second * 5
        one_hundred_milliseconds = 0.1 * epoch_time_unit.multiplier_of_second
        return (receive_time_column - event_time_column).between(-one_hundred_milliseconds, five_seconds).all()

    @staticmethod
    def are_first_and_last_timestamps_within_60_seconds_from_the_borders(series: pd.Series, date: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        day_length = 86_400 * epoch_time_unit.multiplier_of_second
        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1
        sixty_seconds = 1 * epoch_time_unit.multiplier_of_second * 60

        first_timestamp = series.iloc[0]
        last_timestamp = series.iloc[-1]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + sixty_seconds
        last_within_range = day_end_ms - sixty_seconds <= last_timestamp <= day_end_ms

        return first_within_range and last_within_range

    @staticmethod
    def are_first_and_last_timestamps_within_10_minutes_from_the_borders(series: pd.Series, date: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        day_length = 86_400 * epoch_time_unit.multiplier_of_second
        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1
        sixty_seconds = 1 * epoch_time_unit.multiplier_of_second * 60 * 10

        first_timestamp = series.iloc[0]
        last_timestamp = series.iloc[-1]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + sixty_seconds
        last_within_range = day_end_ms - sixty_seconds <= last_timestamp <= day_end_ms

        return first_within_range and last_within_range

    @staticmethod
    def is_transaction_time_lower_or_equal_event_time_with_one_ms_tolerance(transaction_series: pd.Series, event_time_series: pd.Series, epoch_time_unit: EpochTimeUnit) -> bool:
        one_millisecond = epoch_time_unit.multiplier_of_second * 0.001
        return (transaction_series <= event_time_series + one_millisecond).all()

    @staticmethod
    def are_series_values_increasing(series: pd.Series) -> bool:
        return series.diff().dropna().gt(0).all()

    @staticmethod
    def is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(first_update_id: pd.Series, final_update_id: pd.Series) -> bool:
        return (first_update_id.drop_duplicates().iloc[1:] == final_update_id.drop_duplicates().shift(1).iloc[1:] + 1).all()

    @staticmethod
    def is_final_update_id_equal_to_previous_entry_final_update(final_update_id: pd.Series, final_update_id_in_last_stream: pd.Series) -> bool:
        final_update_id = final_update_id.loc[final_update_id.shift(-1) != final_update_id]
        final_update_id_in_last_stream = final_update_id_in_last_stream.loc[final_update_id_in_last_stream.shift(-1) != final_update_id_in_last_stream]
        """
        to consider

        if we do drop_duplicates and end up with different lengths of dataframes,

        does that automatically mean we have an incorrect dataframe?

        Answer after a moment of thought:
        the uniqueness of the combination of FinalUpdateId along with FinalUpdateIdInLastStream means equal dataframe length after performing drop_duplicates()
        and fk it, time for CS, we’ll worry about it later
        """
        return (final_update_id.iloc[:-1].reset_index(drop=True) == final_update_id_in_last_stream.iloc[1:].reset_index(drop=True)).all()

    @staticmethod
    def are_values_with_specified_type(series: pd.Series, expected_type: type) -> bool:
        return series.map(lambda x: type(x) is expected_type).all()

    @staticmethod
    def are_values_positive(series: pd.Series) -> bool:
        return series.gt(0).all()

    @staticmethod
    def are_values_non_negative(series: pd.Series):
        return series.ge(0).all()

    @staticmethod
    def are_values_within_reasonable_range(series: pd.Series, min_value: float, max_value: float) -> bool:
        return series.between(min_value, max_value).all()

    @staticmethod
    def is_there_no_abnormal_price_tick_higher_than_2_percent(series: pd.Series, max_percent_change: float = 2.0) -> bool:
        pct_changes = series.pct_change().dropna() * 100
        return pct_changes.abs().le(max_percent_change).all()

    @staticmethod
    def are_values_zero_or_one(series: pd.Series) -> bool:
        return series.isin([0, 1]).all() and series.map(lambda x: type(x) is int).all()

    @staticmethod
    def is_each_trade_id_bigger_by_one_than_previous(series: pd.Series) -> bool:
        return series.diff()[1:].eq(1).all()

    @staticmethod
    def is_each_snapshot_price_level_amount_accurate_to_market(df: pd.DataFrame, asset_parameters: AssetParameters) -> bool:
        if asset_parameters.stream_type is not StreamType.DEPTH_SNAPSHOT:
            raise Exception('is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT')

        limits_per_side = {
            Market.SPOT: 5000,
            Market.USD_M_FUTURES: 1000,
            Market.COIN_M_FUTURES: 1000
        }
        limit_per_side = limits_per_side[asset_parameters.market]

        price_level_counts = df.groupby(['LastUpdateId', 'IsAsk']).size()

        return (price_level_counts == limit_per_side).all()


'''
    # TRADES CHECK

    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_receive_time_column_close_to_event_time_column_by_minus_100_ms_plus_5_s
            are_first_and_last_timestamp_within_60_seconds_from_the_borders
            
    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid

    ::["data"]["T"] 'TransactionTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_transaction_time_lower_or_equal_event_time

    ::["data"]["s"] 'Symbol' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["t"] 'TradeId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_series_values_increasing
            is_each_trade_id_bigger_by_one_than_previous

    ::["data"]["p"] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_non_negative
            are_values_within_reasonable_range
    ::["data"]["p"] 'Price' [SPOT]
            is_there_no_abnormal_price_tick_higher_than_2_percent
    ::["data"]["p"] 'Price' [USD_M_FUTURES, COIN_M_FUTURES]
            are_values_positive (filtered to XUnknownParameter)
            is_there_no_abnormal_price_tick_higher_than_2_percent (filtered to XUnknownParameter)

    ::["data"]["q"] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_non_negative
            are_values_within_reasonable_range
    ::["data"]["q"] 'Quantity' [USD_M_FUTURES, COIN_M_FUTURES]
            are_values_positive (filtered to XUnknownParameter)

    ::["data"]["m"] 'IsBuyerMarketMaker' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_zero_or_one

    ::["data"]["X"] 'MUnknownParameter' [SPOT]
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["X"] 'XUnknownParameter' [USD_M_FUTURES, COIN_M_FUTURES]
            is_whole_series_made_of_set_of_expected_values
'''

'''
    # DIFFERENCE DEPTH CHECK

    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_receive_time_column_close_to_event_time_column_by_minus_100_ms_plus_5_s
            are_first_and_last_timestamp_within_60_seconds_from_the_borders

    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid

    ::["data"]["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_transaction_time_lower_or_equal_event_time

    ::["data"]["s"] 'Symbol' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["U"] 'FirstUpdateId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
    ::["data"]["U"] 'FirstUpdateId' [SPOT]
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id

    ::["data"]["u"] 'FinalUpdateId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
    ::["data"]["u"] 'FinalUpdateId' [SPOT]
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id

    ::["data"]["pu"] 'FinalUpdateIdInLastStream' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_final_update_id_to_previous_entry_final_update

    ::["data"]["b"]/["data"]["a"] 'IsAsk' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_zero_or_one

    ::["data"]["b"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_positive
            are_values_within_reasonable_range

    ::["data"]["ps"] 'PSUnknownField' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["b"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_non_negative
            are_values_within_reasonable_range
'''

'''
    # DEPTH SNAPSHOT CHECK

    ::["_rc"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            are_first_and_last_timestamp_within_60_seconds_from_the_borders
    ::["_rc"] 'TimestampOfReceive' [USD_M_FUTURES, COIN_M_FUTURES]
        is_receive_time_column_close_to_event_time_column_by_minus_100_ms_plus_5_s

    ::["_rq"] 'TimestampOfRequest' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid           

    ::["E"] 'MessageOutputTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid

    ::["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_transaction_time_lower_or_equal_event_time

    ::["lastUpdateId"] 'LastUpdateId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing

    ::["symbol"] 'Symbol' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["pair"] 'Pair' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["bids"][0]/["asks"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_positive
            are_values_within_reasonable_range

    ::["bids"][1]/["asks"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type
            are_values_non_negative
            are_values_within_reasonable_range
            
    ::MISC
            is_price_level_amount_equal_to_market_amount_limit 
            context of is_price_level_amount_equal_to_market_amount_limit:
            
            base_urls = {
                Market.SPOT: 'https://api.binance.com/api/v3/depth?symbol={}&limit={}',
                Market.USD_M_FUTURES: 'https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={}',
                Market.COIN_M_FUTURES: 'https://dapi.binance.com/dapi/v1/depth?symbol={}&limit={}'
            }
    
            limits = {
                Market.SPOT: 5000,
                Market.USD_M_FUTURES: 1000,
                Market.COIN_M_FUTURES: 1000
                }	
'''

'''
is_there_only_one_unique_value_in_series
is_whole_series_made_of_only_one_expected_value
is_series_non_decreasing
is_whole_series_epoch_valid
are_all_within_utc_z_day_range
is_receive_time_column_close_to_event_time_column_by_minus_100_ms_plus_5_s
are_first_and_last_timestamp_within_60_seconds_from_the_borders
is_transaction_time_lower_or_equal_event_time
are_series_values_increasing
is_first_update_id_bigger_by_one_than_previous_entry_final_update_id
is_final_update_id_to_previous_entry_final_update
are_values_with_specified_type
are_values_positive
are_values_non_negative
are_values_within_reasonable_range
is_there_no_abnormal_price_tick_higher_than_2_percent
are_values_zero_or_one
is_each_trade_id_bigger_by_one_than_previous
are_values_positive_x_column_filtered_to_market
is_whole_series_made_of_set_of_expected_values
'''
