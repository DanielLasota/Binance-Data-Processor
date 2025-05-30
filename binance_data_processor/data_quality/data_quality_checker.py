from __future__ import annotations

import os

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.data_quality.data_quality_report import DataQualityReport
from binance_data_processor.data_quality.individual_column_checker import IndividualColumnChecker as icc
from binance_data_processor.utils.file_utils import (
    decode_asset_parameters_from_csv_name,
    get_base_of_root_csv_filename,
    list_files_in_specified_directory,
    get_csv_len
)
from binance_data_processor.utils.time_utils import get_yesterday_date


def get_dataframe_quality_report(dataframe: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
    data_checker = DataQualityChecker()
    return data_checker.get_dataframe_quality_report(
        dataframe=dataframe,
        asset_parameters=asset_parameters
    )

def conduct_data_quality_analysis_on_whole_directory(csv_nest_directory: str) -> None:
    data_checker = DataQualityChecker()
    data_checker.conduct_whole_directory_of_csvs_data_quality_analysis(csv_nest_directory)

def conduct_data_quality_analysis_on_specified_csv_list(csv_paths: list[str]) -> None:
    data_checker = DataQualityChecker()
    data_checker.conduct_csv_files_data_quality_analysis(csv_paths)

def get_merged_csv_quality_report(csvs_nest_catalog: str, dataframe: pd.DataFrame, asset_parameters_list: list[AssetParameters]) -> list[DataQualityReport]:
    data_checker = DataQualityChecker()

    return data_checker.get_main_merged_csv_report(
        df=dataframe,
        asset_parameters_list=asset_parameters_list,
        csvs_nest_catalog=csvs_nest_catalog
    )


class DataQualityChecker:
    __slots__ = ()

    def __init__(self):
        ...

    @staticmethod
    def print_logo():
        print(f'\033[35m{binance_archiver_logo}')
        print(f'\033[36m')

    def get_dataframe_quality_report(self, dataframe: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        stream_type_handlers = {
            StreamType.DIFFERENCE_DEPTH_STREAM: self._get_full_difference_depth_dataframe_report,
            StreamType.TRADE_STREAM: self._get_full_trade_dataframe_report,
            StreamType.DEPTH_SNAPSHOT: self._get_full_depth_snapshot_dataframe_report
        }
        handler = stream_type_handlers.get(asset_parameters.stream_type)
        return handler(
            df=dataframe.astype({'Price': float, 'Quantity': float}),
            asset_parameters=asset_parameters
        )

    def conduct_whole_directory_of_csvs_data_quality_analysis(self, csv_nest_directory: str) -> None:
        self.print_logo()
        local_files = list_files_in_specified_directory(csv_nest_directory)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        print(f"Found {len(local_csv_file_paths)} CSV files out of {len(local_files)} total files")

        self._conduct_quality_analysis_from_csv_paths_list(csv_paths=local_csv_file_paths)

    def conduct_csv_files_data_quality_analysis(self, csv_paths: list[str]):
        self.print_logo()
        self._conduct_quality_analysis_from_csv_paths_list(csv_paths=csv_paths)

    def _conduct_quality_analysis_from_csv_paths_list(self, csv_paths: list[str]) -> None:
        import pandas as pd
        from alive_progress import alive_bar

        data_quality_report_list = []
        with alive_bar(len(csv_paths), force_tty=True, spinner='dots_waves') as bar:
            for csv_path in csv_paths:
                try:
                    csv_name = os.path.basename(csv_path)
                    asset_parameters = decode_asset_parameters_from_csv_name(csv_name)
                    dataframe = pd.read_csv(csv_path, comment='#')
                    dataframe_quality_report = self.get_dataframe_quality_report(
                        dataframe=dataframe,
                        asset_parameters=asset_parameters
                    )
                    file_name = os.path.basename(csv_path)
                    dataframe_quality_report.file_name = file_name
                    data_quality_report_list.append(dataframe_quality_report)

                    bar()
                except Exception as e:
                    print(f'data_quality_checker.py _conduct_quality_analysis_from_csv_paths_list, {csv_path}: {e}')

        data_quality_positive_report_list = [
            report for report
            in data_quality_report_list
            if report.is_data_quality_report_positive()
        ]

        positive_reports_percentage = len(data_quality_positive_report_list)/len(data_quality_report_list)*100
        print(f'Positive/All: {len(data_quality_positive_report_list)}/{len(data_quality_report_list)} ({positive_reports_percentage}%)')

        i = 1
        for report in data_quality_report_list:
            print(f'{i}. {report.get_data_report_status()}: {report.file_name}')
            i+=1

        while True:
            try:
                user_input = input('\n(n/exit): ').strip().lower()
                if user_input == 'exit':
                    break
                print(data_quality_report_list[int(user_input)-1])
            except Exception as e:
                print(e)

    def get_main_merged_csv_report(self, df: pd.DataFrame, asset_parameters_list: list[AssetParameters], csvs_nest_catalog: str) -> list[DataQualityReport]:

        merged_csv_data_quality_reports_list: list[DataQualityReport] = []
        for ap in asset_parameters_list:
            _df = df[
                (df['StreamType'] == ap.stream_type.name)
                & (df['Market'] == ap.market.name)
                & (df['Symbol'] == ap.pairs[0].upper())
                ]
            _source_csv_report = self.get_dataframe_quality_report(dataframe=_df, asset_parameters=ap)
            merged_csv_data_quality_reports_list.append(_source_csv_report)

        len_of_specified_root_csvs_combined_for_merged_csv = self._new_get_len_of_specified_root_csvs_combined_for_merged_csv(
            asset_parameters_list=asset_parameters_list,
            csvs_nest_catalog=csvs_nest_catalog
        )

        # print(f'len_of_specified_root_csvs_combined_for_merged_csv: {len_of_specified_root_csvs_combined_for_merged_csv}')
        # print(f'df.shape[0]: {df.shape[0]}')

        general_merged_report = DataQualityReport(asset_parameters=asset_parameters_list, df_shape=df.shape)
        is_series_non_decreasing = icc.is_series_non_decreasing(df[df['StreamType'] != 'FINAL_DEPTH_SNAPSHOT']['TimestampOfReceiveUS'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceiveUS'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df[df['StreamType'] != 'FINAL_DEPTH_SNAPSHOT']['TimestampOfReceiveUS'], date=asset_parameters_list[0].date, epoch_time_unit=EpochTimeUnit.MICROSECONDS)
        is_merged_df_len_equal_to_root_csvs_combined = len_of_specified_root_csvs_combined_for_merged_csv == df.shape[0]
        is_islast_column_valid_for_merged = icc.is_islast_column_valid_for_merged(df)
        is_snapshot_injection_valid = icc.is_snapshot_injection_valid_for_merged(df)
        is_whole_set_of_merged_csvs_data_quality_report_positive = all(report.is_data_quality_report_positive() for report in merged_csv_data_quality_reports_list)
        general_merged_report.add_test_result("TimestampOfReceiveUS", "is_series_non_decreasing", is_series_non_decreasing)
        general_merged_report.add_test_result("TimestampOfReceiveUS", "is_series_epoch_valid", is_series_epoch_valid)
        general_merged_report.add_test_result("TimestampOfReceiveUS", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        general_merged_report.add_test_result("GENERAL", "is_merged_df_len_equal_to_root_csvs_combined", is_merged_df_len_equal_to_root_csvs_combined)
        general_merged_report.add_test_result("GENERAL", "is_islast_column_valid_for_merged", is_islast_column_valid_for_merged)
        general_merged_report.add_test_result("GENERAL", "is_snapshot_injection_valid", is_snapshot_injection_valid)
        general_merged_report.add_test_result("GENERAL", "is_whole_set_of_merged_csvs_data_quality_report_positive", is_whole_set_of_merged_csvs_data_quality_report_positive)
        print(f'DataQualityReport: {general_merged_report.get_data_report_status()}')

        return [general_merged_report] + merged_csv_data_quality_reports_list

    @staticmethod
    def _get_full_difference_depth_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        report = DataQualityReport(asset_parameters=asset_parameters, df_shape=df.shape)

        epoch_time_unit = EpochTimeUnit.MICROSECONDS if asset_parameters.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_2_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=2, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_2_seconds_from_the_borders", are_first_and_last_timestamps_within_2_seconds_from_the_borders)
        elif asset_parameters.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=5, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_5_seconds_from_the_borders", are_first_and_last_timestamps_within_5_seconds_from_the_borders)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Stream'], f"{asset_parameters.pairs[0]}@depth@100ms")
        report.add_test_result("Stream", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['EventType'], "depthUpdate")
        report.add_test_result("EventType", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['EventTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['EventTime'])
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("EventTime", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("EventTime", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("EventTime", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("EventTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
            report.add_test_result("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
            report.add_test_result("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
            report.add_test_result("TransactionTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
        report.add_test_result("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['FirstUpdateId'])
        report.add_test_result("FirstUpdateId", "is_series_non_decreasing", is_series_non_decreasing)
        if asset_parameters.market is Market.SPOT:
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id = icc.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            report.add_test_result("FirstUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_first_update_id_bigger_by_one_than_previous_entry_final_update_id)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['FinalUpdateId'])
        report.add_test_result("FinalUpdateId", "is_series_non_decreasing", is_series_non_decreasing)
        if asset_parameters.market is Market.SPOT:
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id = icc.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            report.add_test_result("FinalUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_first_update_id_bigger_by_one_than_previous_entry_final_update_id)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['FinalUpdateIdInLastStream'])
            is_final_update_id_equal_to_previous_entry_final_update = icc.is_final_update_id_equal_to_previous_entry_final_update(df['FinalUpdateId'], df['FinalUpdateIdInLastStream'])
            report.add_test_result("FinalUpdateIdInLastStream", "is_series_non_decreasing", is_series_non_decreasing)
            report.add_test_result("FinalUpdateIdInLastStream", "is_final_update_id_equal_to_previous_entry_final_update", is_final_update_id_equal_to_previous_entry_final_update)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsAsk'])
        report.add_test_result("IsAsk", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
        is_series_range_reasonable_greater_than_or_equal_0_less_than_1e6_values = icc.is_series_range_reasonable_greater_than_or_equal_min_less_than_max_values(df['Price'], 0.0, 1e6)
        report.add_test_result("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        report.add_test_result("Price", "is_series_of_positive_values", is_series_of_positive_values)
        report.add_test_result("Price", "is_series_range_reasonable_greater_than_or_equal_0_less_than_1e6_values", is_series_range_reasonable_greater_than_or_equal_0_less_than_1e6_values)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['PSUnknownField'], asset_parameters.pairs[0].replace('_perp', '').upper())
            report.add_test_result("PSUnknownField", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
        is_series_of_non_negative_values = icc.is_series_of_non_negative_values(df['Quantity'])
        is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_or_equal_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
        report.add_test_result("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        report.add_test_result("Quantity", "is_series_of_non_negative_values", is_series_of_non_negative_values)
        report.add_test_result("Quantity", "is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values", is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values)

        return report

    @staticmethod
    def _get_full_depth_snapshot_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        report = DataQualityReport(asset_parameters=asset_parameters, df_shape=df.shape)

        epoch_time_unit = EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        # is_last_timestamp_within_5400_seconds_from_the_utc_date_end = icc.is_last_timestamp_within_n_seconds_from_the_utc_date_end(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=5400, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms =  icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        report.add_test_result("TimestampOfReceive", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
        # report.add_test_result("TimestampOfReceive", "is_last_timestamp_within_5400_seconds_from_the_utc_date_end", is_last_timestamp_within_5400_seconds_from_the_utc_date_end)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['MessageOutputTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms)
            report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfRequest'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfRequest'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfRequest'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfRequest'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        # is_last_timestamp_within_5400_seconds_from_the_utc_date_end = icc.is_last_timestamp_within_n_seconds_from_the_utc_date_end(df['TimestampOfRequest'], date=asset_parameters.date, n_seconds=5400, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms =  icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfRequest", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("TimestampOfRequest", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("TimestampOfRequest", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        report.add_test_result("TimestampOfRequest", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
        # report.add_test_result("TimestampOfRequest", "is_last_timestamp_within_5400_seconds_from_the_utc_date_end", is_last_timestamp_within_5400_seconds_from_the_utc_date_end)
        report.add_test_result("TimestampOfRequest", "is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['MessageOutputTime'], event_time_column=df['TimestampOfRequest'], x_ms=-3000, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['TransactionTime'], event_time_column=df['TimestampOfRequest'], x_ms=-5000, y_ms=3000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfRequest", "is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms)
            report.add_test_result("TimestampOfRequest", "is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms", is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['MessageOutputTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['MessageOutputTime'])
            is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['MessageOutputTime'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['MessageOutputTime'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end = icc.is_last_timestamp_within_n_seconds_from_the_utc_date_end(df['MessageOutputTime'], date=asset_parameters.date, n_seconds=5400, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['MessageOutputTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['MessageOutputTime'], event_time_column=df['TimestampOfRequest'], x_ms=-3000, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['MessageOutputTime'], df['TransactionTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("MessageOutputTime", "is_series_non_decreasing", is_series_non_decreasing)
            report.add_test_result("MessageOutputTime", "is_series_epoch_valid", is_series_epoch_valid)
            report.add_test_result("MessageOutputTime", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
            report.add_test_result("MessageOutputTime", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
            # report.add_test_result("MessageOutputTime", "is_last_timestamp_within_5400_seconds_from_the_utc_date_end", is_last_timestamp_within_5400_seconds_from_the_utc_date_end)
            report.add_test_result("MessageOutputTime", "is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms)
            report.add_test_result("MessageOutputTime", "is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms)
            report.add_test_result("MessageOutputTime", "is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
            is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TransactionTime'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TransactionTime'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            # is_last_timestamp_within_5400_seconds_from_the_utc_date_end = icc.is_last_timestamp_within_n_seconds_from_the_utc_date_end(df['TransactionTime'], date=asset_parameters.date, n_seconds=5400, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['TransactionTime'], event_time_column=df['TimestampOfRequest'], x_ms=-5000, y_ms=3000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['MessageOutputTime'], df['TransactionTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
            report.add_test_result("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
            report.add_test_result("TransactionTime", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
            report.add_test_result("TransactionTime", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
            # report.add_test_result("TransactionTime", "is_last_timestamp_within_5400_seconds_from_the_utc_date_end", is_last_timestamp_within_5400_seconds_from_the_utc_date_end)
            report.add_test_result("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms)
            report.add_test_result("TransactionTime", "is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms", is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms)
            report.add_test_result("TransactionTime", "is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['LastUpdateId'])
        report.add_test_result("LastUpdateId", "is_series_non_decreasing", is_series_non_decreasing)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
            report.add_test_result("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Pair'], asset_parameters.pairs[0].replace('_perp', '').upper())
            report.add_test_result("Pair", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsAsk'])
        report.add_test_result("IsAsk", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
        is_series_range_reasonable_greater_than_0_less_than_1e6_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Price'], 0.0, 1e6)
        report.add_test_result("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        report.add_test_result("Price", "is_series_of_positive_values", is_series_of_positive_values)
        report.add_test_result("Price", "is_series_range_reasonable_greater_than_0_less_than_1e6_values", is_series_range_reasonable_greater_than_0_less_than_1e6_values)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Quantity'])
        is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
        report.add_test_result("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        report.add_test_result("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
        report.add_test_result("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)

        if asset_parameters.market == Market.SPOT:
            is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side = icc.is_each_snapshot_price_level_amount_in_specified_range(df[['LastUpdateId', 'IsAsk']], asset_parameters, expected_minimum_amount=1000, expected_maximum_amount=5000)
            report.add_test_result("GENERAL", "is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side", is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side = icc.is_each_snapshot_price_level_amount_in_specified_range(df[['LastUpdateId', 'IsAsk']], asset_parameters, expected_minimum_amount=300, expected_maximum_amount=1000)
            report.add_test_result("GENERAL", "is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side", is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side)

        return report

    @staticmethod
    def _get_full_trade_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        import numpy as np
        report = DataQualityReport(asset_parameters=asset_parameters, df_shape=df.shape)

        epoch_time_unit = EpochTimeUnit.MICROSECONDS if asset_parameters.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        if asset_parameters.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_10_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=10, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_10_seconds_from_the_borders", are_first_and_last_timestamps_within_10_seconds_from_the_borders)
        elif asset_parameters.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_minutes_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=5 * 60, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_5_minutes_from_the_borders", are_first_and_last_timestamps_within_5_minutes_from_the_borders)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Stream'], f"{asset_parameters.pairs[0]}@trade")
        report.add_test_result("Stream", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['EventType'], "trade")
        report.add_test_result("EventType", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['EventTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['EventTime'])
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("EventTime", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("EventTime", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("EventTime", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        report.add_test_result("EventTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
        is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
        report.add_test_result("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
        report.add_test_result("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
        report.add_test_result("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        report.add_test_result("TransactionTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
        report.add_test_result("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_each_series_value_bigger_by_one_than_previous = icc.is_each_series_value_bigger_by_one_than_previous(df['TradeId'])
        report.add_test_result("TradeId", "is_each_series_value_bigger_by_one_than_previous", is_each_series_value_bigger_by_one_than_previous)

        """
        TimestampOfReceive,Stream,EventType,EventTime,TransactionTime,Symbol,TradeId,Price,Quantity,IsBuyerMarketMaker,XUnknownParameter
        1741748001578,trxusdt@trade,trade,1741748001573,1741748001573,TRXUSDT,551999400,0.22393,933,1,MARKET
        1741748002452,trxusdt@trade,trade,1741748002447,1741748002447,TRXUSDT,551999401,0,0,0,NA
        1741748002497,trxusdt@trade,trade,1741748002492,1741748002492,TRXUSDT,551999402,0.22394,1,0,MARKET

        XUnknownParameter could be NA then Price n Quantity become 0
        """

        if asset_parameters.market is Market.SPOT:
            is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e6_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Price'], 0.0, 1e6)
            is_there_no_abnormal_tick_higher_than_2_percent = icc.is_there_no_abnormal_tick_higher_than_2_percent(df['Price'])
            report.add_test_result("Price", "is_series_of_positive_values", is_series_of_positive_values)
            report.add_test_result("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            report.add_test_result("Price", "is_series_range_reasonable_greater_than_0_less_than_1e6_values", is_series_range_reasonable_greater_than_0_less_than_1e6_values)
            report.add_test_result("Price", "is_there_no_abnormal_tick_higher_than_2_percent", is_there_no_abnormal_tick_higher_than_2_percent)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_of_positive_values = icc.is_series_of_positive_values(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df[df['XUnknownParameter'] == 'MARKET']['Price'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e6_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df[df['XUnknownParameter'] == 'MARKET']['Price'], 0.0, 1e6)
            is_there_no_abnormal_tick_higher_than_2_percent = icc.is_there_no_abnormal_tick_higher_than_2_percent(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            report.add_test_result("Price", "is_series_of_positive_values", is_series_of_positive_values)
            report.add_test_result("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            report.add_test_result("Price", "is_series_range_reasonable_greater_than_0_less_than_1e6_values", is_series_range_reasonable_greater_than_0_less_than_1e6_values)
            report.add_test_result("Price", "is_there_no_abnormal_tick_higher_than_2_percent", is_there_no_abnormal_tick_higher_than_2_percent)

        if asset_parameters.market is Market.SPOT:
            is_series_of_positive_values = icc.is_series_of_positive_values(df['Quantity'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
            report.add_test_result("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
            report.add_test_result("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            report.add_test_result("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_of_positive_values = icc.is_series_of_positive_values(df[df['XUnknownParameter'] == 'MARKET']['Quantity'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], 0.0, 1e9)
            report.add_test_result("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
            report.add_test_result("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            report.add_test_result("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsBuyerMarketMaker'])
        report.add_test_result("IsBuyerMarketMaker", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        if asset_parameters.market == Market.SPOT:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['MUnknownParameter'], True)
            report.add_test_result("MUnknownParameter", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na = icc.is_series_of_set_of_expected_values(df['XUnknownParameter'], {"MARKET", "INSURANCE_FUND", 'NA', np.nan})
            report.add_test_result("XUnknownParameter", "is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na", is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na)

        return report

    # @staticmethod
    # def _get_full_final_depth_snapshot_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
    #     report = DataQualityReport(asset_parameters=asset_parameters, df_shape=df.shape)
    #     root_csv_difference_depth_df = pd.read_csv()
    #     epoch_time_unit = EpochTimeUnit.MICROSECONDS if asset_parameters.market is Market.SPOT else EpochTimeUnit.MILLISECONDS
    #
    #     is_final_depth_snapshot_equal_to_root_difference_depth_csv_filtered_to_last_price = icc.is_final_depth_snapshot_equal_to_root_difference_depth_csv_filtered_to_last_price(cpp_binance_orderbook_final_depth_snapshot=df, root_csv_difference_depth_df=df)
    #     report.add_test_result('GENERAL', 'is_final_depth_snapshot_equal_to_root_difference_depth_csv_filtered_to_last_price', is_final_depth_snapshot_equal_to_root_difference_depth_csv_filtered_to_last_price)
    #
    #     return report

    @staticmethod
    def _new_get_len_of_specified_root_csvs_combined_for_merged_csv(asset_parameters_list: list[AssetParameters], csvs_nest_catalog: str) -> int:
        import pandas as pd
        # import cpp_binance_orderbook
        # orderbook_session_simulator = cpp_binance_orderbook.OrderBookSessionSimulator()

        total_rows_of_root_csv_combined = total_rows_of_final_depth_snapshot = 0
        for asset_parameters in asset_parameters_list:
            source_csv_path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameters)}.csv'

            if asset_parameters.stream_type is StreamType.DEPTH_SNAPSHOT:
                _df = pd.read_csv(source_csv_path, comment='#')
                _df=_df[_df['TimestampOfReceive'] == _df['TimestampOfReceive'].iloc[0]]
                total_rows_of_root_csv_combined += (_df.shape[0])
            else:
                total_rows_of_root_csv_combined += get_csv_len(source_csv_path)

            if asset_parameters.stream_type is StreamType.DIFFERENCE_DEPTH_STREAM:
                asset_parameter_of_yesterday = AssetParameters(
                    market=asset_parameters.market,
                    stream_type=asset_parameters.stream_type,
                    pairs=asset_parameters.pairs,
                    date=get_yesterday_date(asset_parameters.date)
                )
                source_yesterday_difference_depth_csv_path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter_of_yesterday)}.csv'
                # final_orderbook_snapshot = orderbook_session_simulator.compute_final_depth_snapshot(source_csv_path)
                # total_rows_of_final_depth_snapshot += (len(final_orderbook_snapshot.bids()) + len(final_orderbook_snapshot.asks()))
                df = pd.read_csv(source_yesterday_difference_depth_csv_path, comment='#')
                df = df.drop_duplicates(subset=['IsAsk', 'Price'], keep='last')
                df = df[df['Quantity'] != 0]
                bids = df[df['IsAsk'] == 0].sort_values(by='Price', ascending=False)
                asks = df[df['IsAsk'] == 1].sort_values(by='Price', ascending=True)
                df = pd.concat([bids, asks], ignore_index=True)
                total_rows_of_final_depth_snapshot += df.shape[0]

        return total_rows_of_root_csv_combined + total_rows_of_final_depth_snapshot
