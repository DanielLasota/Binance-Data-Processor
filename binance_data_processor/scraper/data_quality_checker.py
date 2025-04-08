from __future__ import annotations

import os

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.scraper.data_quality_report import DataQualityReport
from binance_data_processor.scraper.individual_column_checker import IndividualColumnChecker


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
            df=dataframe.astype(
                {
                    'Price': float,
                    'Quantity': float
                }
            ),
            asset_parameters=asset_parameters
        )

    def conduct_whole_directory_of_csvs_data_quality_analysis(self, csv_nest_directory: str) -> None:
        self.print_logo()
        local_files = self._list_files_in_local_directory(csv_nest_directory)
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
                    asset_parameters = self._decode_asset_parameters_from_csv_name(csv_name)
                    dataframe = pd.read_csv(csv_path, comment='#')
                    dataframe_quality_report = self.get_dataframe_quality_report(
                        dataframe=dataframe,
                        asset_parameters=asset_parameters
                    )
                    file_name = os.path.basename(csv_path)
                    dataframe_quality_report.set_file_name(file_name)
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

    @staticmethod
    def _list_files_in_local_directory(directory_path: str) -> list:
        try:
            files = []
            for root, _, filenames in os.walk(directory_path):
                for filename in filenames:
                    full_path = os.path.join(root, filename)
                    files.append(full_path)
            return files

        except Exception as e:
            print(f"Error whilst listing files: {e}")
            return []

    @staticmethod
    def _decode_asset_parameters_from_csv_name(csv_name: str) -> AssetParameters:
        _csv_name = csv_name.replace('.csv', '')

        market_mapping = {
            'spot': Market.SPOT,
            'usd_m_futures': Market.USD_M_FUTURES,
            'coin_m_futures': Market.COIN_M_FUTURES,
        }

        stream_type_mapping = {
            'difference_depth': StreamType.DIFFERENCE_DEPTH_STREAM,
            'trade': StreamType.TRADE_STREAM,
            'depth_snapshot': StreamType.DEPTH_SNAPSHOT,
        }

        market = next((value for key, value in market_mapping.items() if key in _csv_name), None)
        if market is None:
            raise ValueError(f"Unknown market in CSV name: {_csv_name}")

        stream_type = next((value for key, value in stream_type_mapping.items() if key in _csv_name), None)
        if stream_type is None:
            raise ValueError(f"Unknown stream type in CSV name: {_csv_name}")

        pair = (
            f"{_csv_name.split('_')[-3]}_{_csv_name.split('_')[-2]}"
            if market is Market.COIN_M_FUTURES
            else _csv_name.split('_')[-2]
        )

        date = _csv_name.split('_')[-1]

        return AssetParameters(
            market=market,
            stream_type=stream_type,
            pairs=[pair],
            date=date
        )

    @staticmethod
    def _get_full_difference_depth_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        report = DataQualityReport(asset_parameters=asset_parameters)
        epoch_time_unit = EpochTimeUnit.MICROSECONDS if asset_parameters.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_timestamp_of_receive_column_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TimestampOfReceive'])
        is_timestamp_of_receive_column_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TimestampOfReceive'])
        are_all_event_time_within_utc_z_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['EventTime'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_timestamp_of_receive_column_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_whole_series_epoch_valid", is_timestamp_of_receive_column_epoch_valid)
        report.add_test_result("TimestampOfReceive", "are_all_within_utc_z_day_range", are_all_event_time_within_utc_z_day_range)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms)
        if asset_parameters.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_2_seconds_from_the_borders = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=2, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_2_seconds_from_the_borders", are_first_and_last_timestamps_within_2_seconds_from_the_borders)
        elif asset_parameters.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_seconds_from_the_borders = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=5, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_5_seconds_from_the_borders", are_first_and_last_timestamps_within_5_seconds_from_the_borders)

        is_there_only_one_unique_value_in_stream_column = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Stream'])
        is_whole_stream_column_made_of_only_one_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Stream'], f"{asset_parameters.pairs[0]}@depth@100ms")
        report.add_test_result("Stream", "is_there_only_one_unique_value_in_series", is_there_only_one_unique_value_in_stream_column)
        report.add_test_result("Stream", "is_whole_series_made_of_only_one_expected_value", is_whole_stream_column_made_of_only_one_expected_value)

        is_there_only_one_unique_value_in_event_type_column = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['EventType'])
        is_whole_event_type_column_made_of_only_one_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['EventType'], "depthUpdate")
        report.add_test_result("EventType", "is_there_only_one_unique_value_in_series", is_there_only_one_unique_value_in_event_type_column)
        report.add_test_result("EventType", "is_whole_series_made_of_only_one_expected_value", is_whole_event_type_column_made_of_only_one_expected_value)

        is_event_time_column_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['EventTime'])
        is_event_time_column_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['EventTime'])
        report.add_test_result("EventTime", "is_series_non_decreasing", is_event_time_column_non_decreasing)
        report.add_test_result("EventTime", "is_whole_series_epoch_valid", is_event_time_column_epoch_valid)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['EventTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
            report.add_test_result("EventTime", "is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms", is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_transaction_time_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TransactionTime'])
            is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['EventTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
            is_transaction_time_column_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TransactionTime'])
            report.add_test_result("TransactionTime", "is_series_non_decreasing", is_transaction_time_non_decreasing)
            report.add_test_result("TransactionTime", "is_whole_series_epoch_valid", is_transaction_time_column_epoch_valid)
            report.add_test_result("TransactionTime", "is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms", is_timestamp_of_event_time_no_greater_than_transaction_time_column_by_one_s_and_no_less_by_1_ms)

        is_there_only_one_unique_value_in_symbol_column = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Symbol'])
        is_whole_symbol_column_made_of_only_one_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
        report.add_test_result("Symbol", "is_there_only_one_unique_value_in_series", is_there_only_one_unique_value_in_symbol_column)
        report.add_test_result("Symbol", "is_whole_series_made_of_only_one_expected_value", is_whole_symbol_column_made_of_only_one_expected_value)

        are_first_update_id_values_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['FirstUpdateId'])
        report.add_test_result("FirstUpdateId", "is_series_non_decreasing", are_first_update_id_values_non_decreasing)
        if asset_parameters.market is Market.SPOT:
            is_first_update_id_bigger_by_one_than_previous_final = IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            report.add_test_result("FirstUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_first_update_id_bigger_by_one_than_previous_final)

        are_final_update_id_values_increasing = IndividualColumnChecker.is_series_non_decreasing(df['FinalUpdateId'])
        report.add_test_result("FinalUpdateId", "is_series_non_decreasing", are_final_update_id_values_increasing)
        if asset_parameters.market is Market.SPOT:
            is_final_update_id_bigger_by_one_than_previous = IndividualColumnChecker.is_first_update_id_bigger_by_one_than_previous_entry_final_update_id(df['FirstUpdateId'], df['FinalUpdateId'])
            report.add_test_result("FinalUpdateId", "is_first_update_id_bigger_by_one_than_previous_entry_final_update_id", is_final_update_id_bigger_by_one_than_previous)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            are_final_update_id_in_last_stream_values_increasing = IndividualColumnChecker.is_series_non_decreasing(df['FinalUpdateIdInLastStream'])
            is_final_update_id_in_last_stream_equal_to_previous = IndividualColumnChecker.is_final_update_id_equal_to_previous_entry_final_update(df['FinalUpdateId'], df['FinalUpdateIdInLastStream'])
            report.add_test_result("FinalUpdateIdInLastStream", "is_series_non_decreasing", are_final_update_id_in_last_stream_values_increasing)
            report.add_test_result("FinalUpdateIdInLastStream", "is_final_update_id_equal_to_previous_entry_final_update", is_final_update_id_in_last_stream_equal_to_previous)

        are_is_ask_values_zero_or_one = IndividualColumnChecker.are_values_zero_or_one(df['IsAsk'])
        report.add_test_result("IsAsk", "are_values_zero_or_one", are_is_ask_values_zero_or_one)

        are_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Price'], float)
        are_price_values_positive = IndividualColumnChecker.are_values_positive(df['Price'])
        are_price_values_within_reasonable_range_zero_to_1e6 = IndividualColumnChecker.are_values_within_reasonable_range(df['Price'], 0.0, 1e6)
        report.add_test_result("Price", "are_values_with_specified_type_of_float", are_values_with_specified_type_of_float)
        report.add_test_result("Price", "are_values_positive", are_price_values_positive)
        report.add_test_result("Price", "are_price_values_within_reasonable_range_zero_to_1e6", are_price_values_within_reasonable_range_zero_to_1e6)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_there_only_one_unique_value_in_ps_unknown_field = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['PSUnknownField'])
            is_whole_ps_unknown_field_made_of_only_one_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['PSUnknownField'], asset_parameters.pairs[0].replace('_perp', '').upper())
            report.add_test_result("PSUnknownField", "is_there_only_one_unique_value_in_series", is_there_only_one_unique_value_in_ps_unknown_field)
            report.add_test_result("PSUnknownField", "is_whole_series_made_of_only_one_expected_value", is_whole_ps_unknown_field_made_of_only_one_expected_value)

        are_quantity_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Quantity'], float)
        are_quantity_values_non_negative = IndividualColumnChecker.are_values_non_negative(df['Quantity'])
        are_quantity_values_within_reasonable_range_zero_to_1e9 = IndividualColumnChecker.are_values_within_reasonable_range(df['Quantity'], 0.0, 1e9)
        report.add_test_result("Quantity", "are_quantity_values_with_specified_type_of_float", are_quantity_values_with_specified_type_of_float)
        report.add_test_result("Quantity", "are_values_non_negative", are_quantity_values_non_negative)
        report.add_test_result("Quantity", "are_quantity_values_within_reasonable_range_zero_to_1e9", are_quantity_values_within_reasonable_range_zero_to_1e9)

        return report

    @staticmethod
    def _get_full_depth_snapshot_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        report = DataQualityReport(asset_parameters=asset_parameters)
        epoch_time_unit = EpochTimeUnit.MILLISECONDS

        is_timestamp_of_receive_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TimestampOfReceive'])
        is_timestamp_of_receive_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TimestampOfReceive'])
        are_all_within_utc_z_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_seconds_from_the_borders = IndividualColumnChecker.are_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms =  IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_timestamp_of_receive_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_whole_series_epoch_valid", is_timestamp_of_receive_epoch_valid)
        report.add_test_result("TimestampOfReceive", "are_all_within_utc_z_day_range", are_all_within_utc_z_day_range)
        report.add_test_result("TimestampOfReceive", "is_first_timestamp_within_60_seconds_from_the_borders", is_first_timestamp_within_60_seconds_from_the_borders)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['MessageOutputTime'], epoch_time_unit=epoch_time_unit))
            is_timestamp_of_receive_no_greater_than_message_transaction_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TransactionTime'], epoch_time_unit=epoch_time_unit))
            report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_message_transaction_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_message_transaction_by_one_s_and_no_less_by_1_ms)

        is_timestamp_of_request_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TimestampOfRequest'])
        is_timestamp_of_request_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TimestampOfRequest'])
        are_all_within_utc_z_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['TimestampOfRequest'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_first_timestamp_within_60_seconds_from_the_borders = IndividualColumnChecker.are_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms =  IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TimestampOfRequest'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfRequest", "is_series_non_decreasing", is_timestamp_of_request_non_decreasing)
        report.add_test_result("TimestampOfRequest", "is_whole_series_epoch_valid", is_timestamp_of_request_epoch_valid)
        report.add_test_result("TimestampOfRequest", "are_all_within_utc_z_day_range", are_all_within_utc_z_day_range)
        report.add_test_result("TimestampOfRequest", "is_first_timestamp_within_60_seconds_from_the_borders", is_first_timestamp_within_60_seconds_from_the_borders)
        report.add_test_result("TimestampOfRequest", "is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_timestamp_of_request_by_one_s_and_no_less_by_1_ms)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive_column=df['TimestampOfRequest'], event_time_column=df['MessageOutputTime'], epoch_time_unit=epoch_time_unit)
            is_timestamp_of_request_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(timestamp_of_receive_column=df['TimestampOfRequest'], event_time_column=df['TransactionTime'], epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfRequest", "is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("TimestampOfRequest", "is_timestamp_of_request_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_request_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_message_output_time_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['MessageOutputTime'])
            is_message_output_time_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['MessageOutputTime'])
            are_all_within_utc_z_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['MessageOutputTime'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_seconds_from_the_borders = IndividualColumnChecker.are_first_timestamp_within_n_seconds_from_the_utc_date_start(df['MessageOutputTime'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['MessageOutputTime'], epoch_time_unit=epoch_time_unit))
            is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfRequest'], df['MessageOutputTime'], epoch_time_unit=epoch_time_unit))
            is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['MessageOutputTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit))
            report.add_test_result("MessageOutputTime", "is_series_non_decreasing", is_message_output_time_non_decreasing)
            report.add_test_result("MessageOutputTime", "is_whole_series_epoch_valid", is_message_output_time_epoch_valid)
            report.add_test_result("MessageOutputTime", "are_all_within_utc_z_day_range", are_all_within_utc_z_day_range)
            report.add_test_result("MessageOutputTime", "is_first_timestamp_within_60_seconds_from_the_borders", is_first_timestamp_within_60_seconds_from_the_borders)
            report.add_test_result("MessageOutputTime", "is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("MessageOutputTime", "is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("MessageOutputTime", "is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)


        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_message_output_time_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TransactionTime'])
            is_message_output_time_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TransactionTime'])
            are_all_within_utc_z_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['TransactionTime'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
            is_first_timestamp_within_60_seconds_from_the_borders = IndividualColumnChecker.are_first_timestamp_within_n_seconds_from_the_utc_date_start(df['TransactionTime'], date=asset_parameters.date, n_seconds=60, epoch_time_unit=epoch_time_unit)
            is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TransactionTime'], epoch_time_unit=epoch_time_unit))
            is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfRequest'], df['TransactionTime'], epoch_time_unit=epoch_time_unit))
            is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = (IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['MessageOutputTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit))
            report.add_test_result("TransactionTime", "is_series_non_decreasing", is_message_output_time_non_decreasing)
            report.add_test_result("TransactionTime", "is_whole_series_epoch_valid", is_message_output_time_epoch_valid)
            report.add_test_result("TransactionTime", "are_all_within_utc_z_day_range", are_all_within_utc_z_day_range)
            report.add_test_result("TransactionTime", "is_first_timestamp_within_60_seconds_from_the_borders", is_first_timestamp_within_60_seconds_from_the_borders)
            report.add_test_result("TransactionTime", "is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("TransactionTime", "is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_request_no_greater_than_message_output_time_by_one_s_and_no_less_by_1_ms)
            report.add_test_result("TransactionTime", "is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_message_output_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)


        is_last_update_id_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['LastUpdateId'])
        report.add_test_result("LastUpdateId", "is_series_non_decreasing", is_last_update_id_non_decreasing)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_symbol_unique = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Symbol'])
            is_symbol_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
            report.add_test_result("Symbol", "is_there_only_one_unique_value_in_series", is_symbol_unique)
            report.add_test_result("Symbol", "is_whole_series_made_of_only_one_expected_value", is_symbol_expected_value)

        if asset_parameters.market == Market.COIN_M_FUTURES:
            is_pair_unique = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Pair'])
            is_pair_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Pair'], asset_parameters.pairs[0].replace('_perp', '').upper())
            report.add_test_result("Pair", "is_there_only_one_unique_value_in_series", is_pair_unique)
            report.add_test_result("Pair", "is_whole_series_made_of_only_one_expected_value", is_pair_expected_value)

        are_is_ask_values_zero_or_one = IndividualColumnChecker.are_values_zero_or_one(df['IsAsk'])
        report.add_test_result("IsAsk", "are_values_zero_or_one", are_is_ask_values_zero_or_one)

        are_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Price'], float)
        are_price_values_positive = IndividualColumnChecker.are_values_positive(df['Price'])
        are_price_values_within_reasonable_range_zero_to_1e6 = IndividualColumnChecker.are_values_within_reasonable_range(df['Price'], 0.0, 1e6)
        report.add_test_result("Price", "are_values_with_specified_type_of_float", are_values_with_specified_type_of_float)
        report.add_test_result("Price", "are_values_positive", are_price_values_positive)
        report.add_test_result("Price", "are_price_values_within_reasonable_range_zero_to_1e6", are_price_values_within_reasonable_range_zero_to_1e6)

        are_quantity_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Quantity'], float)
        are_quantity_values_positive = IndividualColumnChecker.are_values_positive(df['Quantity'])
        are_quantity_values_within_reasonable_range_zero_to_1e9 = IndividualColumnChecker.are_values_within_reasonable_range(df['Quantity'], 0.0, 1e9)
        report.add_test_result("Quantity", "are_quantity_values_with_specified_type_of_float", are_quantity_values_with_specified_type_of_float)
        report.add_test_result("Quantity", "are_values_positive", are_quantity_values_positive)
        report.add_test_result("Quantity", "are_quantity_values_within_reasonable_range_zero_to_1e9", are_quantity_values_within_reasonable_range_zero_to_1e9)

        if asset_parameters.market == Market.SPOT:
            is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side = IndividualColumnChecker.is_each_snapshot_price_level_amount_in_specified_range(df[['LastUpdateId', 'IsAsk']], asset_parameters, expected_minimum_amount=1000, expected_maximum_amount=5000)
            report.add_test_result("GENERAL", "is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side", is_each_snapshot_price_level_amount_in_specified_range_1000_to_5000_per_side)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_price_level_amount_equal_to_market_amount_limit = IndividualColumnChecker.is_each_snapshot_price_level_amount_accurate_to_market(df[['LastUpdateId', 'IsAsk']], asset_parameters, expected_amount_of_price_levels_per_side=1000)
            report.add_test_result("GENERAL", "is_price_level_amount_equal_to_market_amount_limit_of_1000_per_side", is_price_level_amount_equal_to_market_amount_limit)

        return report

    @staticmethod
    def _get_full_trade_dataframe_report(df: pd.DataFrame, asset_parameters: AssetParameters) -> DataQualityReport:
        import numpy as np

        report = DataQualityReport(asset_parameters=asset_parameters)

        epoch_time_unit = EpochTimeUnit.MICROSECONDS if asset_parameters.market is Market.SPOT else EpochTimeUnit.MILLISECONDS

        is_timestamp_of_receive_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TimestampOfReceive'])
        is_timestamp_of_receive_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TimestampOfReceive'])
        are_event_time_within_day_range = IndividualColumnChecker.are_all_within_utc_z_day_range(df['TimestampOfReceive'], date=asset_parameters.date, epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['EventTime'], epoch_time_unit=epoch_time_unit)
        is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("TimestampOfReceive", "is_series_non_decreasing", is_timestamp_of_receive_non_decreasing)
        report.add_test_result("TimestampOfReceive", "is_whole_series_epoch_valid", is_timestamp_of_receive_epoch_valid)
        report.add_test_result("TimestampOfReceive", "are_all_within_utc_z_day_range", are_event_time_within_day_range)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms)
        report.add_test_result("TimestampOfReceive", "is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)
        if asset_parameters.market in [Market.SPOT, Market.USD_M_FUTURES]:
            are_first_and_last_timestamps_within_10_seconds_from_the_borders = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=10, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_10_seconds_from_the_borders", are_first_and_last_timestamps_within_10_seconds_from_the_borders)
        elif asset_parameters.market in [Market.COIN_M_FUTURES]:
            are_first_and_last_timestamps_within_5_minutes_from_the_borders = IndividualColumnChecker.are_first_and_last_timestamps_within_n_seconds_from_the_borders(df['TimestampOfReceive'], date=asset_parameters.date, n_seconds=5 * 60, epoch_time_unit=epoch_time_unit)
            report.add_test_result("TimestampOfReceive", "are_first_and_last_timestamps_within_5_minutes_from_the_borders", are_first_and_last_timestamps_within_5_minutes_from_the_borders)

        is_stream_unique = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Stream'])
        is_stream_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Stream'], f"{asset_parameters.pairs[0]}@trade")
        report.add_test_result("Stream", "is_there_only_one_unique_value_in_series", is_stream_unique)
        report.add_test_result("Stream", "is_whole_series_made_of_only_one_expected_value", is_stream_expected_value)

        is_event_type_unique = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['EventType'])
        is_event_type_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['EventType'], "trade")
        report.add_test_result("EventType", "is_there_only_one_unique_value_in_series", is_event_type_unique)
        report.add_test_result("EventType", "is_whole_series_made_of_only_one_expected_value", is_event_type_expected_value)

        is_event_time_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['EventTime'])
        is_event_time_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['EventTime'])
        is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['EventTime'], epoch_time_unit=epoch_time_unit)
        is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['EventTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("EventTime", "is_series_non_decreasing", is_event_time_non_decreasing)
        report.add_test_result("EventTime", "is_whole_series_epoch_valid", is_event_time_epoch_valid)
        report.add_test_result("EventTime", "is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_event_time_by_one_s_and_no_less_by_1_ms)
        report.add_test_result("EventTime", "is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)

        is_transaction_time_non_decreasing = IndividualColumnChecker.is_series_non_decreasing(df['TransactionTime'])
        is_transaction_time_epoch_valid = IndividualColumnChecker.is_whole_series_epoch_valid(df['TransactionTime'])
        is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['TimestampOfReceive'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
        is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms = IndividualColumnChecker.is_timestamp_of_column_a_no_greater_than_column_b_by_one_s_and_no_less_by_1_ms(df['EventTime'], df['TransactionTime'], epoch_time_unit=epoch_time_unit)
        report.add_test_result("TransactionTime", "is_series_non_decreasing", is_transaction_time_non_decreasing)
        report.add_test_result("TransactionTime", "is_whole_series_epoch_valid", is_transaction_time_epoch_valid)
        report.add_test_result("TransactionTime", "is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_timestamp_of_receive_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)
        report.add_test_result("TransactionTime", "is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms", is_event_time_no_greater_than_transaction_time_by_one_s_and_no_less_by_1_ms)

        is_symbol_unique = IndividualColumnChecker.is_there_only_one_unique_value_in_series(df['Symbol'])
        is_symbol_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['Symbol'], asset_parameters.pairs[0].upper())
        report.add_test_result("Symbol", "is_there_only_one_unique_value_in_series", is_symbol_unique)
        report.add_test_result("Symbol", "is_whole_series_made_of_only_one_expected_value", is_symbol_expected_value)

        are_trade_id_increasing = IndividualColumnChecker.are_series_values_increasing(df['TradeId'])
        is_trade_id_bigger_by_one = IndividualColumnChecker.is_each_trade_id_bigger_by_one_than_previous(df['TradeId'])
        report.add_test_result("TradeId", "are_series_values_increasing", are_trade_id_increasing)
        report.add_test_result("TradeId", "is_each_trade_id_bigger_by_one_than_previous", is_trade_id_bigger_by_one)

        are_price_types_correct = IndividualColumnChecker.are_values_with_specified_type(df['Price'], float)
        are_prices_non_negative = IndividualColumnChecker.are_values_non_negative(df['Price'])
        are_price_in_range = IndividualColumnChecker.are_values_within_reasonable_range(df['Price'], 0.0, 1e9)
        report.add_test_result("Price", "are_values_with_specified_type", are_price_types_correct)
        report.add_test_result("Price", "are_values_non_negative", are_prices_non_negative)
        report.add_test_result("Price", "are_values_within_reasonable_range", are_price_in_range)

        """
        TimestampOfReceive,Stream,EventType,EventTime,TransactionTime,Symbol,TradeId,Price,Quantity,IsBuyerMarketMaker,XUnknownParameter
        1741748001578,trxusdt@trade,trade,1741748001573,1741748001573,TRXUSDT,551999400,0.22393,933,1,MARKET
        1741748002452,trxusdt@trade,trade,1741748002447,1741748002447,TRXUSDT,551999401,0,0,0,NA
        1741748002497,trxusdt@trade,trade,1741748002492,1741748002492,TRXUSDT,551999402,0.22394,1,0,MARKET

        XUnknownParameter could be NA then Price n Quantity become 0
        """

        if asset_parameters.market is Market.SPOT:
            are_price_values_positive = IndividualColumnChecker.are_values_positive(df['Price'])
            are_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Price'], float)
            are_price_values_within_reasonable_range_zero_to_1e6 = IndividualColumnChecker.are_values_within_reasonable_range(df['Price'], 0.0, 1e6)
            is_price_no_abnormal_tick = IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(df['Price'])
            report.add_test_result("Price", "are_values_positive", are_price_values_positive)
            report.add_test_result("Price", "are_values_with_specified_type_of_float", are_values_with_specified_type_of_float)
            report.add_test_result("Price", "are_price_values_within_reasonable_range_zero_to_1e6", are_price_values_within_reasonable_range_zero_to_1e6)
            report.add_test_result("Price", "is_there_no_abnormal_price_tick_higher_than_2_percent", is_price_no_abnormal_tick)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            are_price_values_positive = IndividualColumnChecker.are_values_positive(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            are_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df[df['XUnknownParameter'] == 'MARKET']['Price'], float)
            are_price_values_within_reasonable_range_zero_to_1e6 = IndividualColumnChecker.are_values_within_reasonable_range(df[df['XUnknownParameter'] == 'MARKET']['Price'], 0.0, 1e6)
            is_price_no_abnormal_tick = IndividualColumnChecker.is_there_no_abnormal_price_tick_higher_than_2_percent(df[df['XUnknownParameter'] == 'MARKET']['Price'])
            report.add_test_result("Price", "are_values_positive", are_price_values_positive)
            report.add_test_result("Price", "are_values_with_specified_type_of_float", are_values_with_specified_type_of_float)
            report.add_test_result("Price", "are_price_values_within_reasonable_range_zero_to_1e6", are_price_values_within_reasonable_range_zero_to_1e6)
            report.add_test_result("Price", "is_there_no_abnormal_price_tick_higher_than_2_percent", is_price_no_abnormal_tick)

        if asset_parameters.market is Market.SPOT:
            are_values_positive = IndividualColumnChecker.are_values_positive(df['Quantity'])
            are_quantity_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df['Quantity'], float)
            are_quantity_values_within_reasonable_range_zero_to_1e9 = IndividualColumnChecker.are_values_within_reasonable_range(df['Quantity'], 0.0, 1e9)
            report.add_test_result("Quantity", "ale_values_positive", are_values_positive)
            report.add_test_result("Quantity", "are_quantity_values_with_specified_type_of_float", are_quantity_values_with_specified_type_of_float)
            report.add_test_result("Quantity", "are_quantity_values_within_reasonable_range_zero_to_1e9", are_quantity_values_within_reasonable_range_zero_to_1e9)
        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            are_values_positive = IndividualColumnChecker.are_values_positive(df[df['XUnknownParameter'] == 'MARKET']['Quantity'])
            are_quantity_values_with_specified_type_of_float = IndividualColumnChecker.are_values_with_specified_type(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], float)
            are_quantity_values_within_reasonable_range_zero_to_1e9 = IndividualColumnChecker.are_values_within_reasonable_range(df[df['XUnknownParameter'] == 'MARKET']['Quantity'], 0.0, 1e9)
            report.add_test_result("Quantity", "ale_values_positive", are_values_positive)
            report.add_test_result("Quantity", "are_quantity_values_with_specified_type_of_float", are_quantity_values_with_specified_type_of_float)
            report.add_test_result("Quantity", "are_quantity_values_within_reasonable_range_zero_to_1e9", are_quantity_values_within_reasonable_range_zero_to_1e9)

        are_is_buyer_market_maker_zero_or_one = IndividualColumnChecker.are_values_zero_or_one(df['IsBuyerMarketMaker'])
        report.add_test_result("IsBuyerMarketMaker", "are_values_zero_or_one", are_is_buyer_market_maker_zero_or_one)

        if asset_parameters.market == Market.SPOT:
            is_m_unknown_expected_value = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(df['MUnknownParameter'], True)
            report.add_test_result("MUnknownParameter", "is_whole_series_made_of_only_one_expected_value", is_m_unknown_expected_value)

        if asset_parameters.market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
            is_x_unknown_set_expected_value = IndividualColumnChecker.is_whole_series_made_of_set_of_expected_values(df['XUnknownParameter'], {"MARKET", "INSURANCE_FUND", 'NA', np.nan})
            report.add_test_result("XUnknownParameter", "is_whole_series_made_of_set_of_expected_values_market_insurance_fund_or_na", is_x_unknown_set_expected_value)

        return report
