import io
import pandas as pd

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.scraper.individual_column_checker import IndividualColumnChecker


class TestIndividualColumnChecker:

    def test_given_only_one_unique_value_in_pandas_series_when_is_there_only_one_unique_value_in_series_check_then_true_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=series)
        assert result_of_check == True

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_there_only_one_unique_value_in_series_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'adausdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=series)
        assert result_of_check == False

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'adausdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=series, expected_value='btcusdt@depth@100ms')
        assert result_of_check == False

    def test_given_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=series, expected_value='btcusdt@depth@100ms')
        assert result_of_check == True

    def test_given_pandas_series_with_non_descending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_true_is_being_returned(self):
        series = pd.Series(
            [
                1,
                1,
                2,
                3,
                7,
                11,
                222,
                222
            ]
        )

        result_of_check = IndividualColumnChecker.is_series_non_decreasing(series=series)
        assert result_of_check == True

    def test_given_pandas_series_with_non_ascending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                1,
                1,
                2,
                1,
                7,
                11,
                222,
                222
            ]
        )

        result_of_check = IndividualColumnChecker.is_series_non_decreasing(series=series)
        assert result_of_check == False

    ##### is_whole_series_epoch_valid

    def test_is_whole_series_epoch_milliseconds_valid_positive(self):
        series_ms = pd.Series([1718196460656, 1718196461280, 1718196462000])
        assert IndividualColumnChecker.is_whole_series_epoch_valid(series_ms) == True

    def test_is_whole_series_epoch_microseconds_valid_positive(self):
        series_us = pd.Series([1718196460656000, 1718196461280000, 1718196462000000])
        assert IndividualColumnChecker.is_whole_series_epoch_valid(series_us) == True

    def test_is_whole_series_epoch_milliseconds_valid_negative(self):
        series_ms = pd.Series([1718196460656, -1, 1718196462000])
        assert IndividualColumnChecker.is_whole_series_epoch_valid(series_ms) == False

    def test_is_whole_series_epoch_microseconds_valid_negative(self):
        series_us = pd.Series([1718196460656000, 0, 1718196462000000])
        assert IndividualColumnChecker.is_whole_series_epoch_valid(series_us) == False

    #### are_all_within_utc_z_day_range

    def test_are_all_within_utc_z_day_range_milliseconds_positive(self):
        series = pd.Series([1718150400000, 1718193600000, 1718236799999])
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series, "12-06-2024", EpochTimeUnit.MILLISECONDS) == True

    def test_are_all_within_utc_z_day_range_milliseconds_negative(self):
        series1 = pd.Series([1718150400000, 1718193600000, 1718236800000])
        series2 = pd.Series([1718150399999, 1718193600000, 1718236799999])
        series3 = pd.Series([1718150399999, 1718193600000, 1718236799999])

        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series1, "12-06-2024", EpochTimeUnit.MILLISECONDS) == False
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series2, "12-06-2024", EpochTimeUnit.MILLISECONDS) == False
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series3, "12-06-2024", EpochTimeUnit.MILLISECONDS) == False

    def test_are_all_within_utc_z_day_range_microseconds_positive(self):
        series = pd.Series([1718150400000_000, 1718193600000_000, 1718236799999_999])
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series, "12-06-2024", epoch_time_unit=EpochTimeUnit.MICROSECONDS) == True

    def test_are_all_within_utc_z_day_range_microseconds_negative(self):
        series1 = pd.Series([1718150400000_000, 1718193600000_000, 1718236800000_000])
        series2 = pd.Series([1718150399999_999, 1718193600000_000, 1718236799999_999])
        series3 = pd.Series([1718150399999_999, 1718193600000_000, 1718236800000_000])

        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series1, "12-06-2024", epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series2, "12-06-2024", epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False
        assert IndividualColumnChecker.are_all_within_utc_z_day_range(series3, "12-06-2024", epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False

    ##### is_event_time_column_close_to_receive_time_column_by_100_ms

    def test_is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms_positive_milliseconds(self):
        event_time = pd.Series([1718196460_656, 1718196461_280, 1718196460_656])
        timestamp_of_receive = pd.Series([1718196460_660, 1718196461_290, 1718196460_660])

        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == True

    def test_is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms_positive_microseconds(self):
        event_time = pd.Series([1718196460_656_000, 1718196461_280_000, 1718196460_656_000])
        timestamp_of_receive = pd.Series([1718196460_660_000, 1718196461_290_000, 1718196460_660_000])
        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == True

    def test_is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms_negative_milliseconds(self):
        event_time              = pd.Series([1718196460_656, 1718196461_280])
        timestamp_of_receive    = pd.Series([1718196461_657, 1718196461_280])
        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == False

        event_time              = pd.Series([1718196460_656, 1718196461_280])
        timestamp_of_receive    = pd.Series([1718196460_654, 1718196461_280])
        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == False

    def test_is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms_negative_microseconds(self):
        event_time              = pd.Series([1718196460_656_000, 1718196461_280_000])
        timestamp_of_receive    = pd.Series([1718196461_656_001, 1718196461_280_000])
        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False

        event_time              = pd.Series([1718196460_656_000, 1718196461_280_000])
        timestamp_of_receive    = pd.Series([1718196460_654_999, 1718196461_280_000])
        assert IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive, event_time, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False
    #### are_first_and_last_timestamps_within_5_seconds_from_the_borders

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_positive_milliseconds(self):
        series = pd.Series([
            1718150460000,  # 00:01:00
            1718193600000,  # 12:00:00
            1718236740000  # 23:59:00
        ])
        result = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MILLISECONDS)
        assert result == True, "Expected first and last timestamps to be within 5 seconds from day borders in milliseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_positive_microseconds(self):
        series = pd.Series([
            1718150460000_000,  # 00:01:00
            1718193600000_000,  # 12:00:00
            1718236740000_000  # 23:59:00
        ])
        result = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MICROSECONDS)
        assert result == True, "Expected first and last timestamps to be within 5 seconds from day borders in microseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_negative_milliseconds(self):
        series1 = pd.Series([
            1718150461000,  # 00:01:01 (poza 60s)
            1718193600000,
            1718236740000
        ])
        series2 = pd.Series([
            1718150460000,
            1718193600000,
            1718236739000  # 23:58:59 (poza 60s)
        ])
        series3 = pd.Series([
            1718150461000,  # poza
            1718193600000,
            1718236739000  # poza
        ])

        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series1, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == False, "Expected failure when first timestamp is too late in milliseconds"
        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series2, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == False, "Expected failure when last timestamp is too early in milliseconds"
        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series3, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MILLISECONDS) == False, "Expected failure when both timestamps are out of range in milliseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_negative_microseconds(self):
        series1 = pd.Series([
            1718150461000_000,  # 00:01:01 (poza 60s)
            1718193600000_000,
            1718236740000_000
        ])
        series2 = pd.Series([
            1718150460000_000,
            1718193600000_000,
            1718236739000_000  # 23:58:59 (poza 60s)
        ])
        series3 = pd.Series([
            1718150461000_000,  # poza
            1718193600000_000,
            1718236739000_000  # poza
        ])

        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series1, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False, "Expected failure when first timestamp is too late in microseconds"
        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series2, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False, "Expected failure when last timestamp is too early in microseconds"
        assert IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(series3, date="12-06-2024", n_seconds=60, epoch_time_unit=EpochTimeUnit.MICROSECONDS) == False, "Expected failure when both timestamps are out of range in microseconds"

    ####    are_series_values_increasing

    def test_are_series_values_increasing_positive(self):
        series = pd.Series([1, 2, 3, 4])
        assert IndividualColumnChecker.are_series_values_increasing(series) == True

    def test_are_series_values_increasing_negative(self):
        series = pd.Series([1, 2, 2, 4])
        assert IndividualColumnChecker.are_series_values_increasing(series) == False

    #### is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value

    def test_is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value_positive(self):
        data = """
            FirstUpdateId,FinalUpdateId
            5419254157,5419254159
            5419254160,5419254160
            5419254161,5419254161
            5419254162,5419254165
            5419254166,5419254168
            5419254166,5419254168
            5419254166,5419254168
            5419254166,5419254168
            5419254169,5419254170
            5419254169,5419254170
            5419254171,5419254172
            5419254171,5419254172
            5419254173,5419254175
            5419254173,5419254175
            5419254176,5419254176
            5419254177,5419254177
            5419254178,5419254180
            5419254178,5419254180
            5419254178,5419254180
            5419254181,5419254181
        """
        df = pd.read_csv(io.StringIO(data), skipinitialspace=True)

        assert IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(
            first_update_id=df["FirstUpdateId"],
            final_update_id=df["FinalUpdateId"]
        ) == True

    def test_is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value_negative(self):
        data = """
            FirstUpdateId,FinalUpdateId
            5419254157,5419254159
            5419254160,5419254160
            5419254161,5419254161
            5419254162,5419254165
            5419254166,5419254168
            5419254166,5419254168
            5419254166,5419254168
            5419254166,5419254168
            5419254169,5419254170
            5419254169,5419254170
            5419254171,5419254172
            5419254171,5419254172
            5419254173,5419254175
            5419254173,5419254175
            5419254176,5419254176
            5419254177,5419254177
            5419254179,5419254181
            5419254179,5419254181
            5419254179,5419254181
            5419254182,5419254182
        """
        df = pd.read_csv(io.StringIO(data), skipinitialspace=True)

        assert IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(
            first_update_id=df["FirstUpdateId"],
            final_update_id=df["FinalUpdateId"]
        ) == False

    #### is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_positive(self):
        data = """
                FirstUpdateId,FinalUpdateId,FinalUpdateIdInLastStream
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
        """
        df = pd.read_csv(io.StringIO(data), skipinitialspace=True)
        assert IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df["FinalUpdateId"],
            final_update_id_in_last_stream=df["FinalUpdateIdInLastStream"]
        ) == True

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_negative(self):
        data = """
                FirstUpdateId,FinalUpdateId,FinalUpdateIdInLastStream
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262359,1230163262353
                1230163261501,1230163262359,1230163262353
        """
        df = pd.read_csv(io.StringIO(data), skipinitialspace=True)
        assert IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df["FinalUpdateId"],
            final_update_id_in_last_stream=df["FinalUpdateIdInLastStream"]
        ) == False

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_negative_because_of_deltas_came_mixed(self):
        data = """
                FirstUpdateId,FinalUpdateId,FinalUpdateIdInLastStream
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163245080,1230163246369,1230163244831
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163246700,1230163247546,1230163246369
                1230163247621,1230163248168,1230163247546
                1230163246700,1230163247546,1230163246369
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163247621,1230163248168,1230163247546
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163249410,1230163260013,1230163248168
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
                1230163261501,1230163262351,1230163260013
        """
        df = pd.read_csv(io.StringIO(data), skipinitialspace=True)
        assert IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df["FinalUpdateId"],
            final_update_id_in_last_stream=df["FinalUpdateIdInLastStream"]
        ) == False

    #### are_values_with_specified_type

    def test_are_values_with_specified_type_positive(self):
        series = pd.Series([1.0, 2.5, 3.7])
        assert IndividualColumnChecker.are_values_with_specified_type(series, float) == True

        series = pd.Series([2, 1, 1, 5])
        assert IndividualColumnChecker.are_values_with_specified_type(series, int) == True

        series = pd.Series(["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"])
        assert IndividualColumnChecker.are_values_with_specified_type(series, str) == True

    def test_are_values_with_specified_type_negative(self):
        series = pd.Series([1.0, "2.5", 3.7])
        assert IndividualColumnChecker.are_values_with_specified_type(series, float) == False

        series = pd.Series([2, 1, 1, True])
        assert IndividualColumnChecker.are_values_with_specified_type(series, int) == False

        series = pd.Series(["BTCUSDT", "BTCUSDT", "BTCUSDT", False, None])
        assert IndividualColumnChecker.are_values_with_specified_type(series, str) == False

    #### are_values_with_specified_type

    def test_are_values_positive_positive(self):
        series = pd.Series([1, 2, 3])
        assert IndividualColumnChecker.are_values_positive(series) == True

    def test_are_values_positive_negative(self):
        series = pd.Series([1, 0, 3])
        assert IndividualColumnChecker.are_values_positive(series) == False

    ##### are_values_within_reasonable_range

    def test_are_values_within_reasonable_range_positive(self):
        series = pd.Series([1.5, 2.0, 2.5])
        assert IndividualColumnChecker.are_values_within_reasonable_range(series, 1.0, 3.0) == True

    def test_are_values_within_reasonable_range_negative(self):
        series = pd.Series([1.5, 0.5, 2.5])
        assert IndividualColumnChecker.are_values_within_reasonable_range(series, 1.0, 3.0) == False

    #### is_there_no_abnormal_price_tick_higher_than_2_percent

    def test_is_there_no_abnormal_price_tick_higher_than_2_percent_positive(self):
        series = pd.Series([100, 101, 102])
        assert IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(series) == True

    def test_is_there_no_abnormal_price_tick_higher_than_2_percent_negative(self):
        series = pd.Series([100, 103, 105])  # >2% jump
        assert IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(series) == False

    #### are_values_zero_or_one

    def test_are_values_zero_or_one_positive(self):
        series = pd.Series([0, 1, 0, 1])
        assert IndividualColumnChecker.are_values_zero_or_one(series) == True

    def test_are_values_zero_or_one_negative(self):
        series = pd.Series([0, 1, 2, 1])
        assert IndividualColumnChecker.are_values_zero_or_one(series) == False

        series = pd.Series([0, 1, False, 1])
        assert IndividualColumnChecker.are_values_zero_or_one(series) == False

        series = pd.Series([0, 1, None, 1])
        assert IndividualColumnChecker.are_values_zero_or_one(series) == False

    #### is_each_trade_id_bigger_by_one_than_previous

    def test_is_each_trade_id_bigger_by_one_than_previous_positive(self):
        series = pd.Series([1, 2, 3, 4])
        assert IndividualColumnChecker.is_each_trade_id_bigger_by_one_than_previous(series) == True

    def test_is_each_trade_id_bigger_by_one_than_previous_negative(self):
        series = pd.Series([1, 2, 4, 5])
        assert IndividualColumnChecker.is_each_trade_id_bigger_by_one_than_previous(series) == False

    #### is_each_snapshot_price_level_amount_accurate_to_market

    def test_is_each_snapshot_price_level_amount_accurate_to_market_positive_spot(self):
        data = {
            'LastUpdateId': [1] * 10000,
            'IsAsk': [0] * 5000 + [1] * 5000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=5000)
        assert result == True, "Expected True for SPOT market with exactly 5000 bids and 5000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_positive_usd_m_futures(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.USD_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=1000)
        assert result == True, "Expected True for USD_M_FUTURES market with exactly 1000 bids and 1000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_positive_coin_m_futures(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.COIN_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSD_PERP'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=1000)
        assert result == True, "Expected True for COIN_M_FUTURES market with exactly 1000 bids and 1000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_negative_spot_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 10001,
            'IsAsk': [0] * 5001 + [1] * 5000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=5000)
        assert result == False, "Expected False for SPOT market with 5001 bids (exceeds 5000 limit)"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_negative_usd_m_futures_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 2001,
            'IsAsk': [0] * 1001 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.USD_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=5000)
        assert result == False, "Expected False for USD_M_FUTURES market with 1001 bids (exceeds 1000 limit)"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_negative_multiple_snapshots_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 2000 + [2] * 2001,
            'IsAsk': [0] * 1000 + [1] * 1000 + [0] * 1001 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.COIN_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSD_PERP'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=5000)
        assert result == False, "Expected False for COIN_M_FUTURES market with one snapshot having 1001 bids (exceeds 1000 limit)"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_raises_exception_for_wrong_stream_type(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.TRADE_STREAM,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        try:
            IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df, asset_params, expected_amount_of_price_levels_per_side=5000)
            assert False, "Expected an exception for wrong stream type"
        except Exception as e:
            assert str(
                e) == 'is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT'

    #### is_each_snapshot_price_level_amount_accurate_to_market

    def test_is_each_snapshot_price_level_amount_in_specified_range_positive_spot(self):
        data = {
            'LastUpdateId': [1] * 10000,
            'IsAsk': [0] * 5000 + [1] * 5000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 5000, 5000
        )
        assert result == True, "Expected True for SPOT market with exactly 5000 bids and 5000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_in_specified_range_positive_usd_m_futures(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.USD_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 1000, 1000
        )
        assert result == True, "Expected True for USD_M_FUTURES market with exactly 1000 bids and 1000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_in_specified_range_positive_coin_m_futures(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.COIN_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSD_PERP'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 1000, 1000
        )
        assert result == True, "Expected True for COIN_M_FUTURES market with exactly 1000 bids and 1000 asks for snapshot"

    def test_is_each_snapshot_price_level_amount_in_specified_range_negative_spot_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 10001,
            'IsAsk': [0] * 5001 + [1] * 5000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 5000, 5000
        )
        assert result == False, "Expected False for SPOT market with 5001 bids (exceeds 5000 limit)"

    def test_is_each_snapshot_price_level_amount_in_specified_range_negative_usd_m_futures_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 2001,
            'IsAsk': [0] * 1001 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.USD_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 1000, 1000
        )
        assert result == False, "Expected False for USD_M_FUTURES market with 1001 bids (exceeds 1000 limit)"

    def test_is_each_snapshot_price_level_amount_in_specified_range_negative_multiple_snapshots_exceeds_limit(self):
        data = {
            'LastUpdateId': [1] * 2000 + [2] * 2001,
            'IsAsk': [0] * 1000 + [1] * 1000 + [0] * 1001 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.COIN_M_FUTURES,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSD_PERP'],
            date='01-01-2023'
        )
        result = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df, asset_params, 1000, 1000
        )
        assert result == False, "Expected False for COIN_M_FUTURES market with one snapshot having 1001 bids (exceeds 1000 limit)"

    def test_is_each_snapshot_price_level_amount_in_specified_range_raises_exception_for_wrong_stream_type(self):
        data = {
            'LastUpdateId': [1] * 2000,
            'IsAsk': [0] * 1000 + [1] * 1000
        }
        df = pd.DataFrame(data)
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.TRADE_STREAM,
            pairs=['BTCUSDT'],
            date='01-01-2023'
        )
        try:
            IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
                df, asset_params, 5000, 5000
            )
            assert False, "Expected an exception for wrong stream type"
        except Exception as e:
            assert str(e) == 'is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT'

class TestIndividualColumnCheckerQuantitativeEdition:

    def test_given_only_one_unique_value_in_pandas_series_when_is_there_only_one_unique_value_in_series_check_then_true_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Symbol'])
        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=df['Symbol'])
        assert result_of_check == True

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_there_only_one_unique_value_in_series_check_then_false_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Symbol'])
        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=df['Symbol'])
        assert result_of_check == False

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Stream'])
        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=df['Stream'], expected_value='trxusd_perp@depth@100ms')
        assert result_of_check == True

    def test_given_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Symbol'])

        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=df['Symbol'], expected_value='btcusdt@depth@100ms')
        assert result_of_check == False

    def test_given_pandas_series_with_non_descending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_true_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])

        result_of_check = IndividualColumnChecker.is_series_non_decreasing(series=df['EventTime'])
        assert result_of_check == True

    def test_given_pandas_series_with_non_ascending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_false_is_being_returned(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.is_series_non_decreasing(series=df['EventTime'])
        assert result_of_check == False

##### is_whole_series_epoch_valid

    def test_is_whole_series_epoch_milliseconds_valid_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.is_whole_series_epoch_valid(series=df['EventTime'])
        assert result_of_check == True

    def test_is_whole_series_epoch_microseconds_valid_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['EventTime'])
        series_us = df['EventTime']
        result_of_check = IndividualColumnChecker.is_whole_series_epoch_valid(series=series_us)
        assert result_of_check == True

    def test_is_whole_series_epoch_milliseconds_valid_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['TransactionTime'])
        result_of_check = IndividualColumnChecker.is_whole_series_epoch_valid(series=df['TransactionTime'])
        assert result_of_check == False

    def test_is_whole_series_epoch_microseconds_valid_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.is_whole_series_epoch_valid(series=df['EventTime'])
        assert result_of_check == False

    """Next 4 test needs to be check as there were bug with day before 23:51 timestamps"""

    def test_are_all_within_utc_z_day_range_milliseconds_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.are_all_within_utc_z_day_range(series=df['TimestampOfReceive'], date='04-03-2025')
        assert result_of_check == True

    def test_are_all_within_utc_z_day_range_milliseconds_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.are_all_within_utc_z_day_range(series=df['TimestampOfReceive'], date='04-03-2025')
        assert result_of_check == False

    def test_are_all_within_utc_z_day_range_microseconds_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.are_all_within_utc_z_day_range(series=df['TimestampOfReceive'], date='04-03-2025', epoch_time_unit=EpochTimeUnit.MICROSECONDS)
        assert result_of_check == True

    def test_are_all_within_utc_z_day_range_microseconds_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.are_all_within_utc_z_day_range(series=df['TimestampOfReceive'], date='04-03-2025', epoch_time_unit=EpochTimeUnit.MICROSECONDS)
        assert result_of_check == False

    ##### is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms

    def test_are_event_times_close_to_receive_times_positive_milliseconds(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime', 'TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(
            timestamp_of_receive_column=df['TimestampOfReceive'],
            event_time_column=df['EventTime'],
            epoch_time_unit=EpochTimeUnit.MILLISECONDS
        )

        assert result_of_check == True

    def test_are_event_times_close_to_receive_times_positive_microseconds(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['EventTime', 'TimestampOfReceive'])

        result_of_check = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(
            timestamp_of_receive_column=df['TimestampOfReceive'],
            event_time_column=df['EventTime'],
            epoch_time_unit=EpochTimeUnit.MICROSECONDS
        )

        assert result_of_check == True

    def test_are_event_times_close_to_receive_times_negative_milliseconds(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime', 'TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(
            timestamp_of_receive_column=df['TimestampOfReceive'],
            event_time_column=df['EventTime'],
            epoch_time_unit=EpochTimeUnit.MILLISECONDS
        )

        assert result_of_check == False

    def test_are_event_times_close_to_receive_times_negative_microseconds(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['EventTime', 'TimestampOfReceive'])
        result_of_check = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(
            timestamp_of_receive_column=df['TimestampOfReceive'],
            event_time_column=df['EventTime'],
            epoch_time_unit=EpochTimeUnit.MICROSECONDS
        )

        assert result_of_check == False

    #### are_first_and_last_timestamps_within_5_seconds_from_the_borders

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_positive_milliseconds(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MILLISECONDS,
        )
        assert result_of_check == True, "Expected first and last timestamps to be within 5 seconds from day borders in milliseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_positive_microseconds(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MICROSECONDS,
        )
        assert result_of_check == True, "Expected first and last timestamps to be within 5 seconds from day borders in microseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_negative_milliseconds(self):
        df = pd.read_csv(
            'test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MILLISECONDS,
        )
        assert result_of_check == False, "Expected first or last timestamp to be outside 5 seconds from day borders in milliseconds"

        df = pd.read_csv(
            'test_csvs/test_negative_binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MILLISECONDS
        )
        assert result_of_check == False, "Expected first or last timestamp to be outside 5 seconds from day borders in milliseconds"

    def test_are_first_and_last_timestamp_within_60_seconds_from_the_borders_negative_microseconds(self):
        df = pd.read_csv(
            'test_csvs/test_negative_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MICROSECONDS
        )
        assert result_of_check == False, "Expected first or last timestamp to be outside 5 seconds from day borders in microseconds"

        df = pd.read_csv(
            'test_csvs/test_negative_binance_trade_stream_spot_trxusdt_04-03-2025.csv',
            usecols=['TimestampOfReceive']
        )
        result_of_check = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(
            series=df['TimestampOfReceive'],
            date='04-03-2025',
            n_seconds=60,
            epoch_time_unit=EpochTimeUnit.MICROSECONDS
        )
        assert result_of_check == False, "Expected first or last timestamp to be outside 5 seconds from day borders in microseconds"

    #### are_series_values_increasing

    def test_are_series_values_increasing_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_trade_stream_spot_trxusdt_04-03-2025.csv', usecols=['TradeId'])
        result_of_check = IndividualColumnChecker.are_series_values_increasing(series=df['TradeId'])
        assert result_of_check == True

    def test_are_series_values_increasing_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_trade_stream_spot_trxusdt_04-03-2025.csv', usecols=['TradeId'])
        result_of_check = IndividualColumnChecker.are_series_values_increasing(series=df['TradeId'])
        assert result_of_check == False

    #### is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value
    """Next 2 test needs to consider futures"""

    def test_is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['FirstUpdateId', 'FinalUpdateId'])
        result_of_check = IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(
            first_update_id=df['FirstUpdateId'],
            final_update_id=df['FinalUpdateId']
        )
        assert result_of_check == True

    def test_is_first_update_id_column_value_bigger_by_one_than_previous_entry_final_update_id_column_value_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_spot_trxusdt_04-03-2025.csv', usecols=['FirstUpdateId', 'FinalUpdateId'])
        result_of_check = IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(
            first_update_id=df['FirstUpdateId'],
            final_update_id=df['FinalUpdateId']
        )
        assert result_of_check == False

    #### is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['FinalUpdateId', 'FinalUpdateIdInLastStream'])
        result_of_check = IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df['FinalUpdateId'],
            final_update_id_in_last_stream=df['FinalUpdateIdInLastStream']
        )
        assert result_of_check == True

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['FinalUpdateId', 'FinalUpdateIdInLastStream'])
        result_of_check = IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df['FinalUpdateId'],
            final_update_id_in_last_stream=df['FinalUpdateIdInLastStream']
        )
        assert result_of_check == False

    def test_is_each_current_entry_final_update_id_in_last_stream_equal_to_final_update_from_previous_entry_negative_because_of_deltas_came_mixed(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['FinalUpdateId', 'FinalUpdateIdInLastStream'])
        result_of_check = IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(
            final_update_id=df['FinalUpdateId'],
            final_update_id_in_last_stream=df['FinalUpdateIdInLastStream']
        )
        assert result_of_check == False

    #### are_values_with_specified_type

    def test_are_values_with_specified_type_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.are_values_with_specified_type(series=df['EventTime'], expected_type=int)
        assert result_of_check == True

        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Symbol'])
        result_of_check = IndividualColumnChecker.are_values_with_specified_type(series=df['Symbol'], expected_type=str)
        assert result_of_check == True

    def test_are_values_with_specified_type_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.are_values_with_specified_type(series=df['EventTime'], expected_type=float)
        assert result_of_check == False

        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Symbol'])
        result_of_check = IndividualColumnChecker.are_values_with_specified_type(series=df['Symbol'], expected_type=int)
        assert result_of_check == False

    #### are_values_positive

    def test_are_values_positive_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['EventTime'])
        result_of_check = IndividualColumnChecker.are_values_positive(series=df['EventTime'])
        assert result_of_check == True

    def test_are_values_positive_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Quantity'])
        result_of_check = IndividualColumnChecker.are_values_positive(series=df['Quantity'])
        assert result_of_check == False

    ##### are_values_within_reasonable_range

    def test_are_values_within_reasonable_range_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Price'])
        result_of_check = IndividualColumnChecker.are_values_within_reasonable_range(series=df['Price'], min_value=0, max_value=10)
        assert result_of_check == True

    def test_are_values_within_reasonable_range_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Price'])
        result_of_check = IndividualColumnChecker.are_values_within_reasonable_range(series=df['Price'], min_value=0, max_value=10)
        assert result_of_check == False

    #### is_there_no_abnormal_price_tick_higher_than_2_percent

    def test_is_there_no_abnormal_price_tick_higher_than_2_percent_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Price'])
        result_of_check = IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(series=df['Price'])
        assert result_of_check == True

    def test_is_there_no_abnormal_price_tick_higher_than_2_percent_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['Price'])
        result_of_check = IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(series=df['Price'])
        assert result_of_check == False

    #### are_values_zero_or_one

    def test_are_values_zero_or_one_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['IsAsk'])
        result_of_check = IndividualColumnChecker.are_values_zero_or_one(series=df['IsAsk'])
        assert result_of_check == True

    def test_are_values_zero_or_one_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_difference_depth_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['IsAsk'])
        result_of_check = IndividualColumnChecker.are_values_zero_or_one(series=df['IsAsk'])
        assert result_of_check == False

    #### is_each_trade_id_bigger_by_one_than_previous

    def test_is_each_trade_id_bigger_by_one_than_previous_positive(self):
        df = pd.read_csv('test_csvs/test_positive_binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['TradeId'])
        result_of_check = IndividualColumnChecker.is_each_trade_id_bigger_by_one_than_previous(series=df['TradeId'])
        assert result_of_check == True

    def test_is_each_trade_id_bigger_by_one_than_previous_negative(self):
        df = pd.read_csv('test_csvs/test_negative_binance_trade_stream_coin_m_futures_trxusd_perp_04-03-2025.csv', usecols=['TradeId'])
        result_of_check = IndividualColumnChecker.is_each_trade_id_bigger_by_one_than_previous(series=df['TradeId'])
        assert result_of_check == False

    #### is_each_snapshot_price_level_amount_accurate_to_market

    def test_is_each_snapshot_price_level_amount_accurate_to_market_positive(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        result_of_check = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(
            df=df,
            asset_parameters=asset_params,
            expected_amount_of_price_levels_per_side=5000
        )
        assert result_of_check == True, "Expected True for SPOT market snapshot with exactly 5000 bids and 5000 asks per snapshot"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_negative_exceeds_limit(self):
        df = pd.read_csv(
            'test_csvs/test_negative_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        result_of_check = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(
            df=df,
            asset_parameters=asset_params,
            expected_amount_of_price_levels_per_side=5000
        )
        print(f'result_of_check: {result_of_check}')
        assert result_of_check == False, "Expected False for SPOT market snapshot with at least one side exceeding or below 5000 limit"

    def test_is_each_snapshot_price_level_amount_accurate_to_market_raises_exception_for_wrong_stream_type(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.TRADE_STREAM,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        try:
            IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(
                df=df,
                asset_parameters=asset_params,
                expected_amount_of_price_levels_per_side=5000
            )
            assert False, "Expected an exception for wrong stream type"
        except Exception as e:
            assert str(
                e) == 'is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT'

    #### is_each_snapshot_price_level_amount_accurate_to_market

    def test_is_each_snapshot_price_level_amount_in_specified_range_positive(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        result_of_check = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df=df,
            asset_parameters=asset_params,
            expected_minimum_amount=5000,
            expected_maximum_amount=5000
        )
        assert result_of_check == True, "Expected True for SPOT market snapshot with exactly 5000 bids and 5000 asks per snapshot"

    def test_is_each_snapshot_price_level_amount_in_specified_range_negative_exceeds_limit(self):
        df = pd.read_csv(
            'test_csvs/test_negative_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.DEPTH_SNAPSHOT,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        result_of_check = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
            df=df,
            asset_parameters=asset_params,
            expected_minimum_amount=5000,
            expected_maximum_amount=5000
        )
        print(f'result_of_check: {result_of_check}')
        assert result_of_check == False, "Expected False for SPOT market snapshot with at least one side exceeding or below 5000 limit"

    def test_is_each_snapshot_price_level_amount_in_specified_range_raises_exception_for_wrong_stream_type(self):
        df = pd.read_csv(
            'test_csvs/test_positive_binance_depth_snapshot_spot_btcusdt_11-03-2025.csv',
            usecols=['LastUpdateId', 'IsAsk']
        )
        asset_params = AssetParameters(
            market=Market.SPOT,
            stream_type=StreamType.TRADE_STREAM,
            pairs=['BTCUSDT'],
            date='11-03-2025'
        )
        try:
            IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(
                df=df,
                asset_parameters=asset_params,
                expected_minimum_amount=5000,
                expected_maximum_amount=5000
            )
            assert False, "Expected an exception for wrong stream type"
        except Exception as e:
            assert str(e) == 'is_each_snapshot_price_level_amount_accurate_to_market test is designed for StreamType.DEPTH_SNAPSHOT'
