from __future__ import annotations

from collections import defaultdict

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.data_quality.data_quality_checker import get_merged_csv_quality_report
from binance_data_processor.utils.file_utils import (
    prepare_dump_path_catalog,
    list_files_in_specified_directory,
    decode_asset_parameters_from_csv_name,
    get_base_of_root_csv_filename,
    get_base_of_merged_csv_filename,
    save_df_with_data_quality_reports
)
from binance_data_processor.utils.time_utils import (
    generate_dates_string_list_from_range,
    get_yesterday_date
)

__all__ = [
    'make_concatenated_csvs'
]


def make_concatenated_csvs(
    date_range: list[str],
    pairs: list[str],
    markets: list[str],
    stream_types: list[str],
    should_join_pairs_into_one_csv: bool = False,
    should_join_markets_into_one_csv: bool = False,
    csvs_nest_catalog: str | None = None,
    dump_catalog: str | None = None
):
    orderbook_concatenator = OrderBookConcatenator()

    orderbook_concatenator.run_concatenation(
        date_range=date_range,
        pairs=pairs,
        markets=[Market(market.lower()) for market in markets],
        stream_types=[StreamType(stream_type.lower()) for stream_type in stream_types],
        should_join_pairs_into_one_csv=should_join_pairs_into_one_csv,
        should_join_markets_into_one_csv=should_join_markets_into_one_csv,
        csvs_nest_catalog=csvs_nest_catalog,
        dump_catalog=dump_catalog
    )


class OrderBookConcatenator:

    __slots__ = []

    def __init__(self):
        ...

    def run_concatenation(
            self,
            date_range: list[str],
            pairs: list[str],
            markets: list[Market],
            stream_types: list[StreamType],
            should_join_pairs_into_one_csv: bool = False,
            should_join_markets_into_one_csv: bool = False,
            csvs_nest_catalog: str = 'C:/Users/daniel/Documents/binance_archival_data/',
            dump_catalog: str = 'C:/Users/daniel/Documents/merged_csvs/'
    ):

        prepare_dump_path_catalog(dump_catalog)

        existing_asset_parameters_list = (
            OrderBookConcatenator._get_list_of_asset_parameters_of_files_that_exists_in_specified_directory(
                csvs_nest_catalog
            )
        )

        list_of_asset_parameters_list_to_be_concatenated = (
            OrderBookConcatenator._get_list_of_asset_parameters_list_to_be_concatenated(
                date_range=date_range,
                pairs=pairs,
                markets=markets,
                stream_types=stream_types,
                should_join_pairs_into_one_csv=should_join_pairs_into_one_csv,
                should_join_markets_into_one_csv=should_join_markets_into_one_csv
            )
        )

        unreachable_csv_asset_parameter_list = []
        for csv in list_of_asset_parameters_list_to_be_concatenated:
            for asset_parameter in csv:
                if asset_parameter not in existing_asset_parameters_list:
                    unreachable_csv_asset_parameter_list.append(asset_parameter)

        if unreachable_csv_asset_parameter_list:
            missing_csv_str = "\n".join(str(asset_parameter) for asset_parameter in unreachable_csv_asset_parameter_list)
            raise Exception(f"Missing csv of parameters:\n{missing_csv_str}")

        OrderBookConcatenator._main_concatenate_loop(
            list_of_asset_parameters_list=list_of_asset_parameters_list_to_be_concatenated,
            csvs_nest_catalog=csvs_nest_catalog,
            dump_catalog=dump_catalog
        )

    @staticmethod
    def _get_list_of_asset_parameters_of_files_that_exists_in_specified_directory(csv_nest: str) -> list[AssetParameters]:
        local_files = list_files_in_specified_directory(csv_nest)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        found_asset_parameter_list = []

        for csv in local_csv_file_paths:
            try:
                asset_parameters = decode_asset_parameters_from_csv_name(csv)
                found_asset_parameter_list.append(asset_parameters)
            except Exception as e:
                print(f'_get_existing_files_asset_parameters_list: decode_asset_parameters_from_csv_name sth bad happened: \n {e}')

        return found_asset_parameter_list

    @staticmethod
    def _get_list_of_asset_parameters_list_to_be_concatenated(
            date_range: list[str],
            pairs: list[str],
            markets: list[Market],
            stream_types: list[StreamType],
            should_join_pairs_into_one_csv: bool = False,
            should_join_markets_into_one_csv: bool = False
    ) -> list[list[AssetParameters]]:

        dates = generate_dates_string_list_from_range(date_range)

        groups = []

        # Przypadek 1: Nie łączymy par, nie łączymy rynków – grupujemy wg (pair, market, date)
        if not should_join_pairs_into_one_csv and not should_join_markets_into_one_csv:
            for pair in pairs:
                pair = pair.lower()
                for market in markets:
                    for date in dates:
                        group = []
                        for stream in stream_types:
                            asset_param = AssetParameters(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                        groups.append(group)

        # Przypadek 2: Nie łączymy par, łączymy rynki – grupujemy wg (pair, date) (wszystkie rynki w jednej grupie)
        elif not should_join_pairs_into_one_csv and should_join_markets_into_one_csv:
            for pair in pairs:
                pair = pair.lower()
                for date in dates:
                    group = []
                    for market in markets:
                        for stream in stream_types:
                            asset_param = AssetParameters(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                    groups.append(group)

        # Przypadek 3: Łączymy pary, nie łączymy rynków – grupujemy wg (market, date) (wszystkie pary w jednej grupie)
        elif should_join_pairs_into_one_csv and not should_join_markets_into_one_csv:
            for market in markets:
                for date in dates:
                    group = []
                    for pair in pairs:
                        pair = pair.lower()
                        for stream in stream_types:
                            asset_param = AssetParameters(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                    groups.append(group)

        # Przypadek 4: Łączymy pary i łączymy rynki – grupujemy wg daty (wszystkie pary i rynki w jednej grupie)
        else:  # should_join_pairs_into_one_csv and should_join_markets_into_one_csv
            for date in dates:
                group = []
                for pair in pairs:
                    pair = pair.lower()
                    for market in markets:
                        for stream in stream_types:
                            asset_param = AssetParameters(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                groups.append(group)

        return groups

    @staticmethod
    def _main_concatenate_loop(list_of_asset_parameters_list: list[list[AssetParameters]], csvs_nest_catalog: str, dump_catalog: str) -> None:
        for list_of_asset_parameters_for_single_csv in list_of_asset_parameters_list:
            print(f'SINGLE CSV')
            single_csv_df = OrderBookConcatenator._single_target_csv_loop(list_of_asset_parameters_for_single_csv, csvs_nest_catalog)
            single_csv_filename = get_base_of_merged_csv_filename(list_of_asset_parameters_for_single_csv)

            dataframe_quality_report_list = get_merged_csv_quality_report(
                csvs_nest_catalog=csvs_nest_catalog,
                dataframe=single_csv_df,
                asset_parameters_list=list_of_asset_parameters_for_single_csv
            )

            save_df_with_data_quality_reports(
                dataframe=single_csv_df,
                dataframe_quality_reports=dataframe_quality_report_list,
                dump_catalog=dump_catalog,
                filename=single_csv_filename
            )
            print()

    @staticmethod
    def _single_target_csv_loop(list_of_asset_parameters_for_single_csv: list[AssetParameters], csvs_nest_catalog) -> pd.DataFrame:
        dataframes_to_be_merged_within_single_csv = []
        per_market_dict = OrderBookConcatenator._group_asset_parameters_by_market(list_of_asset_parameters_for_single_csv)
        for market, params_for_market in per_market_dict.items():
            per_pair_dict = OrderBookConcatenator._group_asset_parameters_by_pair(params_for_market)
            pairs_to_be_merged = []
            print(f'         └─single market')
            for pair, params_for_pair in per_pair_dict.items():
                merged_orders_and_trades = OrderBookConcatenator._concatenate_difference_depth_with_depth_snapshot_with_trade_within_single_pair(
                    asset_parameters_list_for_single_market=params_for_pair,
                    csvs_nest_catalog=csvs_nest_catalog
                )
                pairs_to_be_merged.append(merged_orders_and_trades)
            merged_pairs_within_single_market = OrderBookConcatenator._concatenate_pairs_within_single_market(pairs_to_be_merged)
            dataframes_to_be_merged_within_single_csv.append(merged_pairs_within_single_market)
        single_target_csv = OrderBookConcatenator._concatenate_markets_within_single_csv(dataframes_to_be_merged_within_single_csv)
        return single_target_csv

    @staticmethod
    def _group_asset_parameters_by_market(asset_parameters: list[AssetParameters]) -> dict[Market, list[AssetParameters]]:
        grouped: dict[Market, list[AssetParameters]] = defaultdict(list)
        for ap in asset_parameters:
            grouped[ap.market].append(ap)
        return dict(grouped)

    @staticmethod
    def _group_asset_parameters_by_pair(asset_parameters: list[AssetParameters]) -> dict[str, list[AssetParameters]]:
        grouped: dict[str, list[AssetParameters]] = defaultdict(list)
        for ap in asset_parameters:
            grouped[ap.pairs[0]].append(ap)
        return dict(grouped)

    @staticmethod
    def _concatenate_difference_depth_with_depth_snapshot_with_trade_within_single_pair(asset_parameters_list_for_single_market: list[AssetParameters], csvs_nest_catalog) -> pd.DataFrame:
        import pandas as pd

        print('                       └─single pair')
        for _ in asset_parameters_list_for_single_market:
            print(f'                                   └─{_}')

        difference_depth_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM), None)
        trade_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.TRADE_STREAM), None)
        depth_snapshot_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DEPTH_SNAPSHOT), None)

        final_orderbook_snapshot_from_cpp_binance_orderbook = OrderBookConcatenator._get_final_orderbook_snapshot_from_cpp_binance_orderbook(difference_depth_asset_parameters, csvs_nest_catalog)
        root_depth_snapshot_dataframe = OrderBookConcatenator._load_depth_snapshot_root_csv(depth_snapshot_asset_parameters, csvs_nest_catalog)
        root_difference_depth_dataframe = OrderBookConcatenator._load_difference_depth_root_csv(difference_depth_asset_parameters, csvs_nest_catalog)
        root_trade_dataframe = OrderBookConcatenator._load_trade_root_csv(trade_asset_parameters, csvs_nest_catalog)

        orders_and_trades_df = pd.concat(
            [
                root_difference_depth_dataframe,
                root_trade_dataframe
            ],
            ignore_index=True
        )
        orders_and_trades_df = orders_and_trades_df.sort_values(by=['TimestampOfReceive', 'StreamType', 'aux_idx']).reset_index(drop=True)
        del root_difference_depth_dataframe, root_trade_dataframe

        insert_condition = (
                (orders_and_trades_df['StreamType'] == 'DIFFERENCE_DEPTH_STREAM') &
                (orders_and_trades_df['FinalUpdateId'] > root_depth_snapshot_dataframe['LastUpdateId'].iloc[0])
        )

        insert_idx = orders_and_trades_df[insert_condition].index[0]

        target_timestamp_of_receive_of_root_depth_snapshot_dataframe = orders_and_trades_df.iloc[insert_idx:]['TimestampOfReceive'].iloc[0]
        root_depth_snapshot_dataframe['TimestampOfReceive'] = target_timestamp_of_receive_of_root_depth_snapshot_dataframe

        if target_timestamp_of_receive_of_root_depth_snapshot_dataframe < final_orderbook_snapshot_from_cpp_binance_orderbook['TimestampOfReceive'].iloc[0]:
            raise Exception(
                'snapshot timestamp < target_timestamp_of_receive_of_root_depth_snapshot_dataframe'
                f"{target_timestamp_of_receive_of_root_depth_snapshot_dataframe} < {final_orderbook_snapshot_from_cpp_binance_orderbook['TimestampOfReceive'].iloc[0]}"
            )

        final_combined_df = pd.concat(
            [
                final_orderbook_snapshot_from_cpp_binance_orderbook,
                orders_and_trades_df.iloc[:insert_idx],
                root_depth_snapshot_dataframe,
                orders_and_trades_df.iloc[insert_idx:]
            ],
            ignore_index=True
        ).reset_index(drop=True)

        del final_orderbook_snapshot_from_cpp_binance_orderbook

        # final_combined_df.to_csv(f'combined_{asset_parameters_list_for_single_market[0].market.name}_{asset_parameters_list_for_single_market[0].date}.csv', index=True)
        final_combined_df['aux_idx'] = range(len(final_combined_df))

        return final_combined_df

    @staticmethod
    def _get_final_orderbook_snapshot_from_cpp_binance_orderbook(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import cpp_binance_orderbook
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        yesterday_date = get_yesterday_date(asset_parameter.date)
        asset_parameter_of_yesterday = AssetParameters(
            market=asset_parameter.market,
            stream_type=asset_parameter.stream_type,
            pairs=asset_parameter.pairs,
            date=yesterday_date
        )

        csv_path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter_of_yesterday)}.csv'
        orderbook_session_simulator = cpp_binance_orderbook.OrderbookSessionSimulator()
        final_orderbook_snapshot = orderbook_session_simulator.getFinalOrderBookSnapshot(csv_path)

        list_of_entries = []

        for side in (final_orderbook_snapshot.bids, final_orderbook_snapshot.asks):
            for entry in side: list_of_entries.append(entry.to_list())

        del final_orderbook_snapshot

        df =  pd.DataFrame(
            list_of_entries,
            columns=[
                "TimestampOfReceive",
                "Stream",
                "EventType",
                "EventTime",
                "TransactionTime",
                "Symbol",
                "FirstUpdateId",
                "FinalUpdateId",
                "FinalUpdateIdInLastStream",
                "IsAsk",
                "Price",
                "Quantity",
                "PSUnknownField"
            ]
        )
        df['StreamType'] = 'FINAL_DEPTH_SNAPSHOT'
        df['Market'] = asset_parameter.market.name
        df['aux_idx'] = range(len(df))

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceive'] *= 1000

        for column in df.columns:
            current_dtype = df[column].dtype
            if is_integer_dtype(current_dtype):
                df[column] = df[column].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[column] = df[column].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[column] = df[column].astype('float64')

        df['TimestampOfReceive'] = df['TimestampOfReceive'].max()

        return df

    @staticmethod
    def _load_depth_snapshot_root_csv(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        file_path_for_csv = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(file_path_for_csv, comment='#')
        df = df[df['TimestampOfRequest'] == df['TimestampOfRequest'].iloc[0]]
        df['StreamType'] = asset_parameter.stream_type.name
        df['Market'] = asset_parameter.market.name
        df['aux_idx'] = range(len(df))

        df['TimestampOfReceive'] *= 1000

        if asset_parameter.market is not Market.COIN_M_FUTURES:
            df['Symbol'] = asset_parameter.pairs[0].upper()

        for column in df.columns:
            current_dtype = df[column].dtype
            if is_integer_dtype(current_dtype):
                df[column] = df[column].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[column] = df[column].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[column] = df[column].astype('float64')

        return df

    @staticmethod
    def _load_difference_depth_root_csv(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        file_path_for_csv = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(file_path_for_csv, comment='#')
        df['StreamType'] = asset_parameter.stream_type.name
        df['Market'] = asset_parameter.market.name
        df['aux_idx'] = range(len(df))

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceive'] *= 1000

        for column in df.columns:
            current_dtype = df[column].dtype
            if is_integer_dtype(current_dtype):
                df[column] = df[column].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[column] = df[column].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[column] = df[column].astype('float64')

        return df

    @staticmethod
    def _load_trade_root_csv(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        file_path_for_csv = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(file_path_for_csv, comment='#')

        df['StreamType'] = asset_parameter.stream_type.name
        df['Market'] = asset_parameter.market.name
        df['aux_idx'] = range(len(df))

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceive'] *= 1000

        for column in df.columns:
            current_dtype = df[column].dtype
            if is_integer_dtype(current_dtype):
                df[column] = df[column].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[column] = df[column].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[column] = df[column].astype('float64')

        return df

    @staticmethod
    def _concatenate_pairs_within_single_market(list_of_single_pair_dataframe: list[pd.DataFrame]):
        import pandas as pd
        from binance_data_processor.data_quality.individual_column_checker import IndividualColumnChecker as icc

        # print(f'_concatenate_pairs_within_single_market: len(list_of_single_pair_dataframe) {len(list_of_single_pair_dataframe)}')

        if len(list_of_single_pair_dataframe) == 1:
            # print(f'_concatenate_pairs_within_single_market: only 1 df')
            return list_of_single_pair_dataframe[0]

        combined_df = pd.concat(list_of_single_pair_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceive', 'Symbol', 'aux_idx'])

        result_ = icc.is_each_series_value_bigger_by_one_than_previous(combined_df[combined_df['Symbol'] == 'TRXUSDT']['aux_idx'])
        result2_ = icc.is_series_non_decreasing(combined_df['TimestampOfReceive'])
        print(f'hujhuj {result_} {result2_}')

        return combined_df

    @staticmethod
    def _concatenate_markets_within_single_csv(list_of_single_market_dataframe: list[pd.DataFrame]):
        import pandas as pd
        from binance_data_processor.data_quality.individual_column_checker import IndividualColumnChecker as icc

        # print(f'_concatenate_markets_within_single_csv: len(list_of_single_market_dataframe) {len(list_of_single_market_dataframe)}')

        if len(list_of_single_market_dataframe) == 1:
            # print(f'_concatenate_markets_within_single_csv: only 1 df')
            return list_of_single_market_dataframe[0]

        combined_df = pd.concat(list_of_single_market_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceive', 'Market', 'Symbol', 'aux_idx'])

        result_ = icc.is_each_series_value_bigger_by_one_than_previous(combined_df[(combined_df['Symbol'] == 'TRXUSDT') & (combined_df['Market'] == 'SPOT')]['aux_idx'])
        result2_ = icc.is_series_non_decreasing(combined_df['TimestampOfReceive'])
        # print(f'hujhuj {result_} {result2_}')

        return combined_df
