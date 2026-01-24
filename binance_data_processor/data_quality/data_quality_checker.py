from __future__ import annotations

import os

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.data_quality.data_quality_report import DataQualityReport
from binance_data_processor.data_quality.individual_column_checker import IndividualColumnChecker as icc
from binance_data_processor.utils.file_utils import decode_asset_parameters_from_single_root_csv_name
from binance_data_processor.utils.file_utils import get_base_of_root_csv_filename
from binance_data_processor.utils.file_utils import list_files_in_specified_directory
from binance_data_processor.utils.file_utils import get_csv_len
from binance_data_processor.utils.time_utils import get_yesterday_date


def get_df_quality_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
    dqc = DataQualityChecker()
    return dqc.calc_quality_report(df, asset_parameters)

def conduct_data_quality_analysis_on_whole_directory(csv_nest_directory: str) -> None:
    dqc = DataQualityChecker()
    dqc.conduct_whole_directory_of_csvs_data_quality_analysis(csv_nest_directory)

def conduct_data_quality_analysis_on_specified_csv_list(csv_paths: list[str]) -> None:
    dqc = DataQualityChecker()
    dqc.conduct_csv_files_data_quality_analysis(csv_paths)

def get_merged_csv_quality_reports(csvs_nest_catalog: str, df: pd.DataFrame, asset_parameters_list: list[AssetParameters]) -> list[DataQualityReport]:
    dqc = DataQualityChecker()
    return dqc.get_main_merged_csv_report(
        df=df,
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

    def calc_quality_report(self, df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        stream_type_handlers = {
            StreamType.DIFFERENCE_DEPTH_STREAM: self._get_difference_depth_report,
            StreamType.TRADE_STREAM: self._get_trade_report,
            StreamType.DEPTH_SNAPSHOT: self._get_depth_snapshot_report
        }
        handler = stream_type_handlers.get(asset_parameters.stream_type)
        return handler(df.astype({'Price': float, 'Quantity': float}), asset_parameters)

    def conduct_whole_directory_of_csvs_data_quality_analysis(self, csv_nest_directory: str) -> None:
        self.print_logo()
        local_files = list_files_in_specified_directory(csv_nest_directory)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        print(f"Found {len(local_csv_file_paths)} CSV files out of {len(local_files)} total files")

        self.conduct_csv_files_data_quality_analysis(local_csv_file_paths)

    def conduct_csv_files_data_quality_analysis(self, csv_paths: list[str]) -> None:
        import pandas as pd
        from alive_progress import alive_bar

        self.print_logo()

        data_quality_report_list = []
        with alive_bar(len(csv_paths), force_tty=True, spinner='dots_waves') as bar:
            for csv_path in csv_paths:
                try:
                    csv_name = os.path.basename(csv_path)
                    asset_parameters = decode_asset_parameters_from_single_root_csv_name(csv_name)
                    df = pd.read_csv(csv_path, comment='#')
                    df_quality_report = self.calc_quality_report(df, asset_parameters)
                    file_name = os.path.basename(csv_path)
                    df_quality_report.file_name = file_name
                    data_quality_report_list.append(df_quality_report)
                    bar()
                except Exception as e:
                    print(f'data_quality_checker.py _conduct_quality_analysis_from_csv_paths_list, {csv_path}: {e}')

        data_quality_positive_report_list = [report for report in data_quality_report_list if report.is_positive()]

        positive_reports_percentage = len(data_quality_positive_report_list)/len(data_quality_report_list)*100
        print(f'Positive/All: {len(data_quality_positive_report_list)}/{len(data_quality_report_list)} ({positive_reports_percentage}%)')

        i = 1
        for report in data_quality_report_list:
            print(f'{i}. {report.get_status()}: {report.file_name}')
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
        isolated_reports = self._get_isolated_reports(df, asset_parameters_list)

        total_len_of_isolated_dfs = self._calc_total_len_of_isolated_dfs(asset_parameters_list, csvs_nest_catalog)

        print(f'x: {total_len_of_isolated_dfs}')
        print(f'df.shape[0]: {df.shape[0]}')

        report = DataQualityReport(asset_parameters_list, df.shape)
        is_series_non_decreasing = icc.is_series_non_decreasing(df[df['StreamType'] != 'FINAL_DEPTH_SNAPSHOT']['TimestampOfReceiveUS'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceiveUS'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df[df['StreamType'] != 'FINAL_DEPTH_SNAPSHOT']['TimestampOfReceiveUS'], date=asset_parameters_list[0].date, epoch_time_unit=EpochTimeUnit.MICROSECONDS)
        is_merged_df_len_equal_to_root_csvs_combined = total_len_of_isolated_dfs == df.shape[0]
        is_islast_column_valid_for_merged = icc.is_islast_column_valid_for_merged(df)
        is_snapshot_injection_valid = icc.is_snapshot_injection_valid_for_merged(df)
        is_whole_set_of_merged_csvs_data_quality_report_positive = all(report.is_positive() for report in isolated_reports)
        report.add("TimestampOfReceiveUS", "is_series_non_decreasing", is_series_non_decreasing)
        report.add("TimestampOfReceiveUS", "is_series_epoch_valid", is_series_epoch_valid)
        report.add("TimestampOfReceiveUS", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        report.add("GENERAL", "is_merged_df_len_equal_to_root_csvs_combined", is_merged_df_len_equal_to_root_csvs_combined)
        report.add("GENERAL", "is_islast_column_valid_for_merged", is_islast_column_valid_for_merged)
        report.add("GENERAL", "is_snapshot_injection_valid", is_snapshot_injection_valid)
        report.add("GENERAL", "is_whole_set_of_merged_csvs_data_quality_report_positive", is_whole_set_of_merged_csvs_data_quality_report_positive)
        print(f'DataQualityReport: {report.get_status()}')

        return [report] + isolated_reports

    def _get_isolated_reports(self, df: pd.DataFrame, asset_parameters_list: list[AssetParameters]) -> list[DataQualityReport]:
        reports = []
        for ap in asset_parameters_list:
            _df = df[
                (df['StreamType'] == ap.stream_type.name)
                & (df['Market'] == ap.market.name)
                & (df['Symbol'] == ap.pairs[0].upper())
            ]
            isolated_report = self.calc_quality_report(_df, ap)
            reports.append(isolated_report)
        return reports

    @staticmethod
    def _get_difference_depth_report(df: pd.DataFrame, ap: AssetParameters) -> DataQualityReport:
        r = DataQualityReport(ap, df.shape)

        epoch_time_unit = EpochTimeUnit.MICROSECONDS if ap.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=ap.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        r.add("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_2_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=ap.date, n_seconds=2, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "are_first_and_last_timestamps_within_2_seconds_from_the_borders", are_first_and_last_timestamps_within_2_seconds_from_the_borders)
        elif ap.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=ap.date, n_seconds=5, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "are_first_and_last_timestamps_within_5_seconds_from_the_borders", are_first_and_last_timestamps_within_5_seconds_from_the_borders)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Stream'], f"{ap.pairs[0]}@depth@100ms")
        r.add("Stream", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['EventType'], "depthUpdate")
        r.add("EventType", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['EventTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['EventTime'])
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        r.add("EventTime", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("EventTime", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("EventTime", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
            r.add("EventTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
            r.add("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
            r.add("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
            r.add("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
            r.add("TransactionTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], ap.pairs[0].upper())
        r.add("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['FirstUpdateId'])
        r.add("FirstUpdateId", "is_series_non_decreasing", is_series_non_decreasing)
        if ap.market is Market.SPOT:
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id = icc.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            r.add("FirstUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_first_update_id_bigger_by_one_than_previous_entry_final_update_id)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['FinalUpdateId'])
        r.add("FinalUpdateId", "is_series_non_decreasing", is_series_non_decreasing)
        if ap.market is Market.SPOT:
            is_first_update_id_bigger_by_one_than_previous_entry_final_update_id = icc.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            r.add("FinalUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_first_update_id_bigger_by_one_than_previous_entry_final_update_id)

        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['FinalUpdateIdInLastStream'])
            is_final_update_id_equal_to_previous_entry_final_update = icc.is_final_update_id_equal_to_previous_entry_final_update(df['FinalUpdateId'], df['FinalUpdateIdInLastStream'])
            r.add("FinalUpdateIdInLastStream", "is_series_non_decreasing", is_series_non_decreasing)
            r.add("FinalUpdateIdInLastStream", "is_final_update_id_equal_to_previous_entry_final_update", is_final_update_id_equal_to_previous_entry_final_update)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsAsk'])
        r.add("IsAsk", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
        is_series_range_reasonable_greater_than_or_equal_0_less_than_1e7_values = icc.is_series_range_reasonable_greater_than_or_equal_min_less_than_max_values(df['Price'], 0.0, 1e7)
        r.add("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        r.add("Price", "is_series_of_positive_values", is_series_of_positive_values)
        r.add("Price", "is_series_range_reasonable_greater_than_or_equal_0_less_than_1e7_values", is_series_range_reasonable_greater_than_or_equal_0_less_than_1e7_values)

        if ap.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['PSUnknownField'], ap.pairs[0].replace('_perp', '').upper())
            r.add("PSUnknownField", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
        is_series_of_non_negative_values = icc.is_series_of_non_negative_values(df['Quantity'])
        is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_or_equal_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
        r.add("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        r.add("Quantity", "is_series_of_non_negative_values", is_series_of_non_negative_values)
        r.add("Quantity", "is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values", is_series_range_reasonable_greater_than_or_equal_0_less_than_1e9_values)

        return r

    @staticmethod
    def _get_depth_snapshot_report(df: pd.DataFrame, ap: AssetParameters) -> DataQualityReport:
        r = DataQualityReport(ap, df.shape)

        epoch_time_unit = EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=ap.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfReceive'], date=ap.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms =  icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
        r.add("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        r.add("TimestampOfReceive", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
        r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['MessageOutputTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms)
            r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfRequest'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfRequest'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfRequest'], date=ap.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfRequest'], date=ap.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms =  icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
        r.add("TimestampOfRequest", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("TimestampOfRequest", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("TimestampOfRequest", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        r.add("TimestampOfRequest", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
        r.add("TimestampOfRequest", "is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_timestamp_of_request_by_1_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['MessageOutputTime'], event_time_column=df['TimestampOfRequest'], x_ms=-3000, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['TransactionTime'], event_time_column=df['TimestampOfRequest'], x_ms=-5000, y_ms=3000, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfRequest", "is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms)
            r.add("TimestampOfRequest", "is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms", is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms)

        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['MessageOutputTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['MessageOutputTime'])
            is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['MessageOutputTime'], date=ap.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['MessageOutputTime'], date=ap.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['MessageOutputTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['MessageOutputTime'], event_time_column=df['TimestampOfRequest'], x_ms=-3000, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['MessageOutputTime'], df['TransactionTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            r.add("MessageOutputTime", "is_series_non_decreasing", is_series_non_decreasing)
            r.add("MessageOutputTime", "is_series_epoch_valid", is_series_epoch_valid)
            r.add("MessageOutputTime", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
            r.add("MessageOutputTime", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
            r.add("MessageOutputTime", "is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_message_output_time_by_1_ms_and_not_greater_by_5000_ms)
            r.add("MessageOutputTime", "is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_timestamp_of_request_by_3000_ms_and_not_greater_by_5000_ms)
            r.add("MessageOutputTime", "is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms)

        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
            is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
            is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TransactionTime'], date=ap.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_s_from_the_utc_date_start = icc.is_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TransactionTime'], date=ap.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
            is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(timestamp_of_receive_column=df['TransactionTime'], event_time_column=df['TimestampOfRequest'], x_ms=-5000, y_ms=3000, epoch_time_unit=epoch_time_unit)
            is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['MessageOutputTime'], df['TransactionTime'], x_ms=-1, y_ms=5000, epoch_time_unit=epoch_time_unit)
            r.add("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
            r.add("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
            r.add("TransactionTime", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
            r.add("TransactionTime", "is_first_timestamp_within_60_s_from_the_utc_date_start", is_first_timestamp_within_60_s_from_the_utc_date_start)
            r.add("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_than_by_5000_ms)
            r.add("TransactionTime", "is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms", is_transaction_time_not_less_than_timestamp_of_request_by_5000_ms_and_not_greater_by_3000_ms)
            r.add("TransactionTime", "is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms", is_message_output_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_5000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['LastUpdateId'])
        r.add("LastUpdateId", "is_series_non_decreasing", is_series_non_decreasing)

        if ap.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], ap.pairs[0].upper())
            r.add("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        if ap.market == Market.COIN_M_FUTURES:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Pair'], ap.pairs[0].replace('_perp', '').upper())
            r.add("Pair", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsAsk'])
        r.add("IsAsk", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
        is_series_range_reasonable_greater_than_0_less_than_1e7_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Price'], 0.0, 1e7)
        r.add("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        r.add("Price", "is_series_of_positive_values", is_series_of_positive_values)
        r.add("Price", "is_series_range_reasonable_greater_than_0_less_than_1e7_values", is_series_range_reasonable_greater_than_0_less_than_1e7_values)

        is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
        is_series_of_positive_values = icc.is_series_of_positive_values(df['Quantity'])
        is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
        r.add("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
        r.add("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
        r.add("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)

        if ap.market == Market.SPOT:
            is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side = icc.is_each_snapshot_price_level_amount_in_specified_range(df[['LastUpdateId', 'IsAsk']], ap, expected_minimum_amount=1000, expected_maximum_amount=5000)
            r.add("GENERAL", "is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side", is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side = icc.is_each_snapshot_price_level_amount_in_specified_range(df[['LastUpdateId', 'IsAsk']], ap, expected_minimum_amount=300, expected_maximum_amount=1000)
            r.add("GENERAL", "is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side", is_each_snapshot_price_level_amount_in_specified_range_300_to_1000_per_side)

        return r

    @staticmethod
    def _get_trade_report(df: pd.DataFrame, ap: AssetParameters) -> DataQualityReport:
        import numpy as np
        r = DataQualityReport(ap, df.shape)

        epoch_time_unit = EpochTimeUnit.MICROSECONDS if ap.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TimestampOfReceive'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TimestampOfReceive'])
        is_series_epoch_within_utc_z_day_range = icc.is_series_epoch_within_utc_z_day_range(df['TimestampOfReceive'], date=ap.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        r.add("TimestampOfReceive", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("TimestampOfReceive", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("TimestampOfReceive", "is_series_epoch_within_utc_z_day_range", is_series_epoch_within_utc_z_day_range)
        r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        r.add("TimestampOfReceive", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        if ap.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_10_seconds_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=ap.date, n_seconds=10, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "are_first_and_last_timestamps_within_10_seconds_from_the_borders", are_first_and_last_timestamps_within_10_seconds_from_the_borders)
        elif ap.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_minutes_from_the_borders = icc.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=ap.date, n_seconds=5 * 60, epoch_time_unit=epoch_time_unit)
            r.add("TimestampOfReceive", "are_first_and_last_timestamps_within_5_minutes_from_the_borders", are_first_and_last_timestamps_within_5_minutes_from_the_borders)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Stream'], f"{ap.pairs[0]}@trade")
        r.add("Stream", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['EventType'], "trade")
        r.add("EventType", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['EventTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['EventTime'])
        is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['EventTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
        r.add("EventTime", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("EventTime", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("EventTime", "is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_event_time_by_5_ms_and_not_greater_by_5000_ms)
        r.add("EventTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_non_decreasing = icc.is_series_non_decreasing(df['TransactionTime'])
        is_series_epoch_valid = icc.is_series_epoch_valid(df['TransactionTime'])
        is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['TimestampOfReceive'], df['TransactionTime'], x_ms=-5, y_ms=5000, epoch_time_unit=epoch_time_unit)
        is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms = icc.is_timestamp_of_column_a_not_less_than_column_b_by_x_ms_and_not_greater_by_y_ms(df['EventTime'], df['TransactionTime'], x_ms=-1, y_ms=3000, epoch_time_unit=epoch_time_unit)
        r.add("TransactionTime", "is_series_non_decreasing", is_series_non_decreasing)
        r.add("TransactionTime", "is_series_epoch_valid", is_series_epoch_valid)
        r.add("TransactionTime", "is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms", is_timestamp_of_receive_not_less_than_transaction_time_by_5_ms_and_not_greater_by_5000_ms)
        r.add("TransactionTime", "is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms", is_event_time_not_less_than_transaction_time_by_1_ms_and_not_greater_by_3000_ms)

        is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['Symbol'], ap.pairs[0].upper())
        r.add("Symbol", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        is_each_series_value_bigger_by_one_than_previous = icc.is_each_series_value_bigger_by_one_than_previous(df['TradeId'])
        r.add("TradeId", "is_each_series_value_bigger_by_one_than_previous", is_each_series_value_bigger_by_one_than_previous)

        """
        TimestampOfReceive,Stream,EventType,EventTime,TransactionTime,Symbol,TradeId,Price,Quantity,IsBuyerMarketMaker,XUnknownParameter
        1741748001578,trxusdt@trade,trade,1741748001573,1741748001573,TRXUSDT,551999400,0.22393,933,1,MARKET
        1741748002452,trxusdt@trade,trade,1741748002447,1741748002447,TRXUSDT,551999401,0,0,0,NA
        1741748002497,trxusdt@trade,trade,1741748002492,1741748002492,TRXUSDT,551999402,0.22394,1,0,MARKET

        XUnknownParameter could be NA then Price n Quantity become 0
        """

        if ap.market is Market.SPOT:
            is_series_of_positive_values = icc.is_series_of_positive_values(df['Price'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Price'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e7_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Price'], 0.0, 1e7)
            is_there_no_abnormal_tick_higher_than_2_percent = icc.is_there_no_abnormal_tick_higher_than_2_percent(df['Price'])
            r.add("Price", "is_series_of_positive_values", is_series_of_positive_values)
            r.add("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            r.add("Price", "is_series_range_reasonable_greater_than_0_less_than_1e7_values", is_series_range_reasonable_greater_than_0_less_than_1e7_values)
            r.add("Price", "is_there_no_abnormal_tick_higher_than_2_percent", is_there_no_abnormal_tick_higher_than_2_percent)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_of_positive_values = icc.is_series_of_positive_values(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df[df['XUnknownParameter'] == 'MARKET']['Price'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e7_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df[df['XUnknownParameter'] == 'MARKET']['Price'], 0.0, 1e7)
            is_there_no_abnormal_tick_higher_than_2_percent = icc.is_there_no_abnormal_tick_higher_than_2_percent(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            r.add("Price", "is_series_of_positive_values", is_series_of_positive_values)
            r.add("Price", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            r.add("Price", "is_series_range_reasonable_greater_than_0_less_than_1e7_values", is_series_range_reasonable_greater_than_0_less_than_1e7_values)
            r.add("Price", "is_there_no_abnormal_tick_higher_than_2_percent", is_there_no_abnormal_tick_higher_than_2_percent)

        if ap.market is Market.SPOT:
            is_series_of_positive_values = icc.is_series_of_positive_values(df['Quantity'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df['Quantity'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df['Quantity'], 0.0, 1e9)
            r.add("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
            r.add("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            r.add("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)
        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_series_of_positive_values = icc.is_series_of_positive_values(df[df['XUnknownParameter'] == 'MARKET']['Quantity'])
            is_series_of_expected_data_type_float = icc.is_series_of_expected_data_type(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], float)
            is_series_range_reasonable_greater_than_0_less_than_1e9_values = icc.is_series_range_reasonable_greater_than_min_less_than_max_values(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], 0.0, 1e9)
            r.add("Quantity", "is_series_of_positive_values", is_series_of_positive_values)
            r.add("Quantity", "is_series_of_expected_data_type_float", is_series_of_expected_data_type_float)
            r.add("Quantity", "is_series_range_reasonable_greater_than_0_less_than_1e9_values", is_series_range_reasonable_greater_than_0_less_than_1e9_values)

        is_series_of_zero_or_one_only = icc.is_series_of_zero_or_one_only(df['IsBuyerMarketMaker'])
        r.add("IsBuyerMarketMaker", "is_series_of_zero_or_one_only", is_series_of_zero_or_one_only)

        if ap.market == Market.SPOT:
            is_series_of_only_one_unique_expected_value = icc.is_series_of_only_one_unique_expected_value(df['MUnknownParameter'], True)
            r.add("MUnknownParameter", "is_series_of_only_one_unique_expected_value", is_series_of_only_one_unique_expected_value)

        if ap.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na = icc.is_series_of_set_of_expected_values(df['XUnknownParameter'], {"MARKET", "INSURANCE_FUND", 'NA', np.nan})
            r.add("XUnknownParameter", "is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na", is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na)

        return r

    @staticmethod
    def _calc_total_len_of_isolated_dfs(asset_parameters_list: list[AssetParameters], csvs_nest_catalog: str) -> int:
        import pandas as pd
        # import cpp_binance_orderbook
        # orderbook_session_simulator = cpp_binance_orderbook.OrderBookSessionSimulator()

        total_rows_of_root_csv_combined = total_rows_of_final_depth_snapshot = 0
        for ap in asset_parameters_list:
            source_csv_path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(ap)}.csv'

            if ap.stream_type is StreamType.DEPTH_SNAPSHOT:
                _df = pd.read_csv(source_csv_path, comment='#')
                _df=_df[_df['TimestampOfReceive'] == _df['TimestampOfReceive'].iloc[0]]
                total_rows_of_root_csv_combined += (_df.shape[0])
            else:
                total_rows_of_root_csv_combined += get_csv_len(source_csv_path)

            if ap.stream_type is StreamType.DIFFERENCE_DEPTH_STREAM:
                asset_parameter_of_yesterday = AssetParameters(
                    market=ap.market,
                    stream_type=ap.stream_type,
                    pairs=ap.pairs,
                    date=get_yesterday_date(ap.date)
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
