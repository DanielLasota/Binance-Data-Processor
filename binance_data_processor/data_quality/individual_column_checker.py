from __future__ import annotations

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.stream_type_enum import StreamType


class IndividualColumnChecker:
    __slots__ = ()

    @staticmethod
    def is_series_of_only_one_unique_expected_value(series: pd.Series, expected_value: any) -> bool:
        return series.unique()[0] == expected_value and len(series.unique()) == 1

    @staticmethod
    def is_series_of_set_of_expected_values(series: pd.Series, expected_values: set[any]) -> bool:
        return set(series.unique()) <= expected_values

    @staticmethod
    def is_series_non_decreasing(series: pd.Series) -> bool:
        return series.diff().min() >= 0

    @staticmethod
    def is_series_epoch_valid(series: pd.Series) -> bool:
        import pandas as pd

        return (
                series.notna().all()
                and series.gt(0).all()
                and series.astype(float).eq(series.astype(int)).all()
                and pd.api.types.is_integer_dtype(series)
        )

    @staticmethod
    def is_series_epoch_within_utc_z_day_range(series: pd.Series, date: str, epoch_time_unit: EpochTimeUnit) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)

        day_length = 86_400 * epoch_time_unit.multiplier_of_second

        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1

        return series.between(day_start_ms, day_end_ms).all()

    @staticmethod
    def is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column: pd.Series, event_time_column: pd.Series, x_ms: int, y_ms: int, epoch_time_unit: EpochTimeUnit) -> bool:
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

        factor = 1_000 if epoch_time_unit is EpochTimeUnit.MICROSECONDS else 1
        x_ms *= factor
        y_ms *= factor
        return (timestamp_of_receive_column - event_time_column).between(x_ms, y_ms).all()

    @staticmethod
    def are_first_and_last_timestamps_within_n_seconds_from_the_borders(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        multiplier_of_second = epoch_time_unit.multiplier_of_second
        day_length = 86_400 * epoch_time_unit.multiplier_of_second
        day_start_ms = int(day_start.timestamp() * multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1
        _n_seconds = multiplier_of_second * n_seconds

        first_timestamp = series.iloc[0]
        last_timestamp = series.iloc[-1]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + _n_seconds
        last_within_range = day_end_ms - _n_seconds <= last_timestamp <= day_end_ms

        return first_within_range and last_within_range

    @staticmethod
    def is_first_timestamp_within_n_seconds_from_the_utc_date_start(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ms = int(day_start.timestamp() * epoch_time_unit.multiplier_of_second)
        _n_seconds = 1 * epoch_time_unit.multiplier_of_second * n_seconds

        first_timestamp = series.iloc[0]

        first_within_range = day_start_ms <= first_timestamp <= day_start_ms + _n_seconds

        return first_within_range

    @staticmethod
    def is_last_timestamp_within_n_seconds_from_the_utc_date_end(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        multiplier_of_second = epoch_time_unit.multiplier_of_second
        day_length = 86_400 * epoch_time_unit.multiplier_of_second
        day_start_ms = int(day_start.timestamp() * multiplier_of_second)
        day_end_ms = day_start_ms + day_length - 1
        _n_seconds = multiplier_of_second * n_seconds

        last_timestamp = series.iloc[-1]

        last_within_range = day_end_ms - _n_seconds <= last_timestamp <= day_end_ms

        return last_within_range

    @staticmethod
    def is_series_within_n_seconds_before_utc_date_end(series: pd.Series, date: str, n_seconds: int, epoch_time_unit: EpochTimeUnit) -> bool:
        import pandas as pd

        day_start = pd.to_datetime(date, format='%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
        multiplier = epoch_time_unit.multiplier_of_second

        day_start_ts = int(day_start.timestamp() * multiplier)
        day_end_ts = day_start_ts + (86_400 * multiplier) - 1

        window_start = day_end_ts - (n_seconds * multiplier)

        return series.between(window_start, day_end_ts).all()

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
    def is_series_of_expected_data_type(series: pd.Series, expected_type: type) -> bool:
        return series.map(lambda x: type(x) is expected_type).all()

    @staticmethod
    def is_series_of_positive_values(series: pd.Series) -> bool:
        return series.gt(0).all()

    @staticmethod
    def is_series_of_non_negative_values(series: pd.Series):
        return series.ge(0).all()

    @staticmethod
    def is_series_range_reasonable_greater_than_min_less_than_max_values(series: pd.Series, min_value: float, max_value: float) -> bool:
        return ((series > min_value) & (series < max_value)).all()

    @staticmethod
    def is_series_range_reasonable_greater_than_or_equal_min_less_than_max_values(series: pd.Series, min_value: float, max_value: float) -> bool:
        return ((series >= min_value) & (series < max_value)).all()

    @staticmethod
    def is_there_no_abnormal_tick_higher_than_2_percent(series: pd.Series, max_percent_change: float = 2.0) -> bool:
        pct_changes = series.pct_change().dropna() * 100
        return pct_changes.abs().le(max_percent_change).all()

    @staticmethod
    def is_series_of_zero_or_one_only(series: pd.Series) -> bool:
        return series.isin([0, 1]).all() and series.map(lambda x: type(x) is int).all()

    @staticmethod
    def is_each_series_value_bigger_by_one_than_previous(series: pd.Series) -> bool:
        # diffs = series.diff()[1:]
        # correct = diffs.eq(1)
        # if not correct.all():
        #     print("Występują błędne różnice w następujących wierszach:")
        #     print(diffs[~correct])

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

    @staticmethod
    def is_islast_column_valid_for_merged(df: pd.DataFrame) -> bool:
        checks = []

        diff = df[df['StreamType'] == 'DIFFERENCE_DEPTH_STREAM']
        grp_diff = diff.groupby(['Market', 'Symbol', 'FinalUpdateId'])['IsLast'].sum()
        checks.append((grp_diff == 1).all())

        snap = df[df['StreamType'] == 'DEPTH_SNAPSHOT']
        grp_snap = snap.groupby(['Market', 'Symbol', 'LastUpdateId'])['IsLast'].sum()
        checks.append((grp_snap == 1).all())

        final = df[df['StreamType'] == 'FINAL_DEPTH_SNAPSHOT']
        grp_final = final.groupby(['Market', 'Symbol'])['IsLast'].sum()
        checks.append((grp_final == 1).all())

        trade = df[df['StreamType'] == 'TRADE_STREAM']
        checks.append(trade['IsLast'].eq(1).all())

        return all(checks)

    @staticmethod
    def is_snapshot_injection_valid_for_merged(df: pd.DataFrame) -> bool:

        if df.empty:
            return True

        for (market, symbol), group in df.groupby(['Market', 'Symbol']):
            grp = group.reset_index(drop=True)
            snaps = grp[grp['StreamType'] == 'DEPTH_SNAPSHOT']
            if snaps.empty:
                continue

            diff_idxs = grp[grp['StreamType'] == 'DIFFERENCE_DEPTH_STREAM'].index.tolist()

            for last_id, snap_group in snaps.groupby('LastUpdateId'):
                idxs = snap_group.index.tolist()
                i0 = idxs[0]
                n = len(idxs)

                # find next diff pos
                # looking in diff_idxs first one that > i0+n-1
                after_snapshot = [i for i in diff_idxs if i > i0 + n - 1]
                if not after_snapshot:
                    print(f"[ERROR] [{market}/{symbol}] last_id={last_id}: brak diff-a po snapshotach")
                    return False
                next_idx = after_snapshot[0]
                next_row = grp.iloc[next_idx]

                # condition FinalUpdateId > last_id
                if next_row['FinalUpdateId'] <= last_id:
                    print(f"[ERROR] [{market}/{symbol}] last_id={last_id}: "
                          f"diff na idx={next_idx} ma FinalUpdateId={next_row['FinalUpdateId']} "
                          f"<= {last_id}")
                    return False

                # find previous diff before i0
                before_snapshot = [i for i in diff_idxs if i < i0]
                if before_snapshot:
                    prev_idx = before_snapshot[-1]
                    prev = grp.iloc[prev_idx]
                    if prev['FinalUpdateId'] > last_id:
                        print(f"[ERROR] [{market}/{symbol}] last_id={last_id}: "
                              f"diff przed snapshot na idx={prev_idx} ma FinalUpdateId={prev['FinalUpdateId']} "
                              f"> {last_id}")
                        return False

                # check timestamps
                ts = next_row['TimestampOfReceiveUS']
                bad = snap_group[snap_group['TimestampOfReceiveUS'] != ts].index.tolist()
                if bad:
                    print(f"[ERROR] [{market}/{symbol}] last_id={last_id}: "
                          f"snapshoty na idx={bad} mają TS różne od {ts}")
                    return False

            # verify diff order monotonic
            diffs = grp[grp['StreamType'] == 'DIFFERENCE_DEPTH_STREAM']
            if not diffs['FinalUpdateId'].is_monotonic_increasing:
                print(f"[ERROR] [{market}/{symbol}]: "
                      f"FinalUpdateId diff-ów nie jest rosnący: "
                      f"{diffs['FinalUpdateId'].tolist()}")
                return False

        return True

''' ----------- APPENDIX: Conventional Names -----------:

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
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms                         (-5ms,+5s)
        [SPOT, USD_M_FUTURES]
            are_first_and_last_timestamps_within_2_seconds_from_the_borders
        [COIN_M_FUTURES]
            are_first_and_last_timestamps_within_5_seconds_from_the_borders
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms                   (-5ms,+5s)
            
    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms                         (-5ms,+5s)
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms                             (-1ms,+3s)

    ::["data"]["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms                   (-5ms,+5s)
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms                             (-1ms,+3s)

    ::["data"]["s"] 'Symbol' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

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
            is_series_of_zero_or_one_only

    ::["data"]["b"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_expected_data_type_float
            is_series_of_positive_values
            is_series_range_reasonable_greater_than_or_equal_0_less_than_1e6_values

    ::["data"]["ps"] 'PSUnknownField' [COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["b"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_expected_data_type_float
            is_series_of_non_negative_values
            is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values
'''

'''
    # DEPTH SNAPSHOT CHECK

    ::["_rc"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_first_timestamp_within_60_s_from_the_utc_date_start
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end
            is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms               (-1ms, +5s)
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms                (-1ms, +5s)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms              (-5ms, +5s)

    ::["_rq"] 'TimestampOfRequest' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_first_timestamp_within_60_s_from_the_utc_date_start
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end
            is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms               (-1ms, +5s)
    ::["_rq"] 'TimestampOfRequest' [USD_M_FUTURES, COIN_M_FUTURES]
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms             (-3s, +5s)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms                (-5s, +3s)

    ::["E"] 'MessageOutputTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_first_timestamp_within_60_s_from_the_utc_date_start
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms                (-1ms, +5s)
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms             (-3s, +5s)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms                    (-1ms, +5s)
            
    ::["T"] 'TransactionTime' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_first_timestamp_within_60_s_from_the_utc_date_start
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms              (-5ms, +5s)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms                (-5s, +3s)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms                    (-1ms, +5s)
            
    ::["lastUpdateId"] 'LastUpdateId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing

    ::["symbol"] 'Symbol' [COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["pair"] 'Pair' [COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value
            
    ::["data"]["b"]/["data"]["a"] 'IsAsk' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_zero_or_one_only

    ::["bids"][0]/["asks"][0] 'Price' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_expected_data_type_float
            is_series_of_positive_values
            is_series_range_reasonable_greater_than_0_less_than_1e6_values

    ::["bids"][1]/["asks"][1] 'Quantity' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_expected_data_type_float
            is_series_of_positive_values
            is_series_range_reasonable_greater_than_0_less_than_1e9_values
            
    ::MISC 'LastUpdateId', 'IsAsk', [SPOT]
            is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side
        [USD_M_FUTURES, COIN_M_FUTURES]
            is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side
'''

'''
    # TRADES CHECK

    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms                         (-5ms, +5s)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms                   (-5ms, +5s)
    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES]
            are_first_and_last_timestamps_within_10_seconds_from_the_borders
    ::["_E"] 'TimestampOfReceive' [COIN_M_FUTURES]
            are_first_and_last_timestamps_within_5_minutes_from_the_borders

    ::["stream"] 'Stream' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["e"] 'EventType' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["E"] 'EventTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms                         (-5ms, +5s)
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms                             (-1ms, +3s)

    ::["data"]["T"] 'TransactionTime' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms                   (-5ms, +5s)
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms                             (-1ms, +3s)
            
    ::["data"]["s"] 'Symbol' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_only_one_unique_expected_value

    ::["data"]["t"] 'TradeId' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_each_series_value_bigger_by_one_than_previous

    ::["data"]["p"] 'Price' [SPOT]
            is_series_of_positive_values
            is_series_of_expected_data_type_float
            is_series_range_reasonable_greater_than_0_less_than_1e6_values
            is_there_no_abnormal_tick_higher_than_2_percent
    ::["data"]["p"] 'Price' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_positive_values                                            df[df['XUnknownParameter'] == 'MARKET']['Price']        
            is_series_of_expected_data_type_float                                   df[df['XUnknownParameter'] == 'MARKET']['Price']
            is_series_range_reasonable_greater_than_0_less_than_1e6_values          df[df['XUnknownParameter'] == 'MARKET']['Price']
            is_there_no_abnormal_tick_higher_than_2_percent                         df[df['XUnknownParameter'] == 'MARKET']['Price']

    ::["data"]["q"] 'Quantity' [SPOT]
            is_series_of_positive_values
            is_series_of_expected_data_type_float
            is_series_range_reasonable_greater_than_0_less_than_1e9_values
    ::["data"]["q"] 'Quantity' [USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_positive_values                                            df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            is_series_of_expected_data_type_float                                   df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            is_series_range_reasonable_greater_than_0_less_than_1e9_values          df[df['XUnknownParameter'] == 'MARKET']['Quantity']
            
    ::["data"]["m"] 'IsBuyerMarketMaker' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_of_zero_or_one_only

    ::["data"]["M"] 'MUnknownParameter' [SPOT]
            is_series_of_only_one_unique_expected_value

    ::["data"]["X"] 'XUnknownParameter' [USD_M_FUTURES, COIN_M_FUTURES]
            is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na
'''

'''
    # FINAL DEPTH SNAPSHOT CHECK
    :TODO

'''

'''
    # MERGED CSV [FINAL_DEPTH_SNAPSHOT, DIFFERENCE_DEPTH_STREAM, TRADE_STREAM, DEPTH_SNAPSHOT]
    
    ::["_E"] 'TimestampOfReceive' [SPOT, USD_M_FUTURES, COIN_M_FUTURES]
            is_series_non_decreasing
            is_series_epoch_valid
            is_series_epoch_within_utc_z_day_range
            is_final_depth_snapshot_within_n_seconds_before_utc_date_end
    GENERAL
            is_merged_df_len_equal_to_single_csvs_combined
            is_islast_column_valid_for_merged
            is_snapshot_injection_valid
            is_whole_set_of_merged_csvs_data_quality_report_positive
    
'''
