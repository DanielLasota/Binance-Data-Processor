from __future__ import annotations

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


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
        import pandas as pd

        return (
                series.notna().all()
                and series.gt(0).all()
                and series.astype(float).eq(series.astype(int)).all()
                and pd.api.types.is_integer_dtype(series)
        )

    @staticmethod
    def are_all_within_utc_z_day_range(series: pd.Series, date: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)

        day_length = 86_400 * epoch_time_unit.multiplier_of_second

        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1

        return series.between(day_start_ms, day_end_ms).all()

    @staticmethod
    def is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive_column: pd.Series, event_time_column: pd.Series, epoch_time_unit: EpochTimeUnit) -> bool:
        """
        func checks drift between TimestampOfReceive and EventTime or other combination:

        it checks if TimestampOfReceive is no greater than EventTime by 1 second

        (ms: epoch 100
        or us epoch 100_000)

        We also assume 1 ms of tolerance so:

        TimestampOfReceive,EventTime
        1743495203952000,1743495203952999

        or

        TimestampOfReceive,EventTime
        1743495203814,1743495203815


        We consider as ok


        TimestampOfReceive,EventTime
        1743495203952,1743495203815
        """

        one_second = epoch_time_unit.multiplier_of_second * 1
        one_millisecond = epoch_time_unit.multiplier_of_second / 1000
        return (timestamp_of_receive_column - event_time_column).between(-one_millisecond, one_second).all()

    @staticmethod
    def are_first_and_last_timestamps_within_n_seconds_from_the_borders(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        day_length = 86_400 * epoch_time_unit.multiplier_of_second
        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1
        sixty_seconds = 1 * epoch_time_unit.multiplier_of_second * n_seconds

        first_timestamp = series.iloc[0]
        last_timestamp = series.iloc[-1]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + sixty_seconds
        last_within_range = day_end_ms - sixty_seconds <= last_timestamp <= day_end_ms

        return first_within_range and last_within_range

    @staticmethod
    def are_first_timestamp_within_n_seconds_from_the_utc_date_start(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        n_seconds = 1 * epoch_time_unit.multiplier_of_second * n_seconds

        first_timestamp = series.iloc[0]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + n_seconds

        return first_within_range

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
    def are_values_exclusively_within_specified_reasonable_range(series: pd.Series, min_value: float, max_value: float) -> bool:
        return ((series > min_value) & (series < max_value)).all()

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
    def is_each_snapshot_price_level_amount_accurate_to_market(df: pd.DataFrame, asset_parameters: AssetParameters, expected_amount_of_price_levels_per_side: int) -> bool:
        if asset_parameters.stream_type is not StreamType.DEPTH_SNAPSHOT:
            raise Exception('is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT')

        price_level_counts = df.groupby(['LastUpdateId', 'IsAsk']).size()

        return (price_level_counts == expected_amount_of_price_levels_per_side).all()

    @staticmethod
    def is_each_snapshot_price_level_amount_in_specified_range(df: pd.DataFrame, asset_parameters: AssetParameters, expected_minimum_amount: int, expected_maximum_amount) -> bool:
        if asset_parameters.stream_type is not StreamType.DEPTH_SNAPSHOT:
            raise Exception('is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT')

        price_level_counts = df.groupby(['LastUpdateId', 'IsAsk']).size()

        return ((price_level_counts >= expected_minimum_amount) & (price_level_counts <= expected_maximum_amount)).all()

'''
    is_timestamp_of_receive_column_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TimestampOfReceive'])
    report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_timestamp_of_receive_column_non_decreasing)

    are_quantity_values_within_reasonable_range_zero_to_1e9 = IndividualColumnChecker.are_values_within_reasonable_range(df['Quantity'], 0.0, 1e9)
    report.add_test_result("Quantity", "are_quantity_values_within_reasonable_range_zero_to_1e9", are_quantity_values_within_reasonable_range_zero_to_1e9)

    # Column                  TestName                                                             Status       
    # TimestampOfReceive      is_series_non_decreasing                                             POSITIVE  
    ...
    # Quantity                are_quantity_values_within_reasonable_range_zero_to_1e9              POSITIVE   
    
    Column: Column
    TestName: are_{column if there are at least 2 vars to check}_values_within_reasonable_range{min, max}
    variable_name: are_{column(s)}_values_within_reasonable_range{min, max}
'''

'''
    # DIFFERENCE DEPTH CHECK

    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms
        [SPOT, USD_M_FUTURES]
            are_first_and_last_timestamps_within_2_seconds_from_the_borders
        [COIN_M_FUTURES]
            are_first_and_last_timestamps_within_5_seconds_from_the_borders
            
    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms

    ::["data"]["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms

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
            is_final_update_id_equal_to_previous_entry_final_update

    ::["data"]["b"]/["data"]["a"] 'IsAsk' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_zero_or_one

    ::["data"]["b"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type_of_float
            are_values_positive
            are_values_exclusively_within_specified_reasonable_range_zero_to_1e6

    ::["data"]["ps"] 'PSUnknownField' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["b"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type_of_float
            are_values_non_negative
            are_values_exclusively_within_specified_reasonable_range_zero_to_1e9
'''

'''
TimestampOfReceive,MessageOutputTime,TransactionTime,TimestampOfRequest

    # DEPTH SNAPSHOT CHECK

    ::["_rc"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_first_timestamp_within_60_seconds_from_the_borders
            is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms
            is_timestamp_of_receive_no_greater_than_message_transaction_time_by_one_s_and_no_less_by_1_ms

    ::["_rq"] 'TimestampOfRequest' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_first_timestamp_within_60_seconds_from_the_borders
            is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms
    ::["_rq"] 'TimestampOfRequest' [USD_M_FUTURES, COIN_M_FUTURES]
            is_message_output_time_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms
            is_transaction_time_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms

    ::["E"] 'MessageOutputTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_first_timestamp_within_60_seconds_from_the_borders
            is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms
            is_message_output_time_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms
            is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms
            
    ::["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_first_timestamp_within_60_seconds_from_the_borders
            is_timestamp_of_receive_no_greater_than_message_transaction_time_by_one_s_and_no_less_by_1_ms
            is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms
            is_transaction_time_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms
            
    ::["lastUpdateId"] 'LastUpdateId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing

    ::["symbol"] 'Symbol' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["pair"] 'Pair' [COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value
            
    ::["data"]["b"]/["data"]["a"] 'IsAsk' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_zero_or_one

    ::["bids"][0]/["asks"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_with_specified_type_of_float
            are_values_positive
            are_values_exclusively_within_specified_reasonable_range_range_zero_to_1e6

    ::["bids"][1]/["asks"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_quantity_values_with_specified_type_of_float
            are_quantity_values_positive
            are_values_exclusively_within_specified_reasonable_range_range_zero_to_1e9
            
    ::MISC 'LastUpdateId', 'IsAsk', [SPOT]
            is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_price_level_amount_equal_to_market_amount_limit_of_1000_per_side
'''

'''
    # TRADES CHECK

    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            are_all_within_utc_z_day_range
            is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms
            is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms
    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES]
            are_first_and_last_timestamps_within_10_seconds_from_the_borders
    ::["_E"] 'TimestampOfReceive' [COIN_M_FUTURES]
            are_first_and_last_timestamps_within_5_minutes_from_the_borders

    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms
            is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms

    ::["data"]["T"] 'TransactionTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_whole_series_epoch_valid
            is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms
            is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms
            
    ::["data"]["s"] 'Symbol' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_there_only_one_unique_value_in_series
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["t"] 'TradeId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_series_values_increasing
            is_each_trade_id_bigger_by_one_than_previous

    ::["data"]["p"] 'Price' [SPOT]
            are_values_positive
            are_values_with_specified_type_of_float
            are_values_exclusively_within_specified_reasonable_range_zero_to_1e6 ( trzeba zmienic na > , nazwe tez)
            is_there_no_abnormal_price_tick_higher_than_2_percent
    ::["data"]["p"] 'Price' [USD_M_FUTURES, COIN_M_FUTURES]
            are_values_positive                                                                     df[df['XUnknownParameter'] == 'MARKET']['Price']        
            are_values_with_specified_type_of_float                                                 df[df['XUnknownParameter'] == 'MARKET']['Price']
            are_values_exclusively_within_specified_reasonable_range_zero_to_1e6                    df[df['XUnknownParameter'] == 'MARKET']['Price']
            is_there_no_abnormal_price_tick_higher_than_2_percent                                   df[df['XUnknownParameter'] == 'MARKET']['Price']

    ::["data"]["q"] 'Quantity' [SPOT]
            are_values_positive
            are_values_with_specified_type_of_float
            are_values_exclusively_within_specified_reasonable_zero_to_1e9
    ::["data"]["q"] 'Quantity' [USD_M_FUTURES, COIN_M_FUTURES]
            are_quantity_values_positive                                                            df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            are_values_with_specified_type_of_float                                                 df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            are_values_exclusively_within_specified_reasonable_zero_to_1e9                          df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            
    ::["data"]["m"] 'IsBuyerMarketMaker' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            are_values_zero_or_one

    ::["data"]["X"] 'MUnknownParameter' [SPOT]
            is_whole_series_made_of_only_one_expected_value

    ::["data"]["X"] 'XUnknownParameter' [USD_M_FUTURES, COIN_M_FUTURES]
            is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na



ustalic wspolny format dla nazw (stringow testow)
przepisac nazwy wszystkich zmiennych na unikalne!!!!!!
import IndividualColumnChecker as icc
'''
