from __future__ import annotations

from collections import defaultdict
import os

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
    'make_merged_csvs'
]


def make_merged_csvs(
    date_range: list[str],
    pairs: list[str],
    markets: list[str],
    stream_types: list[str],
    should_join_pairs_into_one_csv: bool = False,
    should_join_markets_into_one_csv: bool = False,
    csvs_nest_catalog: str | None = None,
    dump_catalog: str | None = None
):
    bdm = BinanceDataMerger()

    bdm.merge(
        date_range=date_range,
        pairs=pairs,
        markets=[Market(market.lower()) for market in markets],
        stream_types=[StreamType(stream_type.lower()) for stream_type in stream_types],
        should_join_pairs_into_one_csv=should_join_pairs_into_one_csv,
        should_join_markets_into_one_csv=should_join_markets_into_one_csv,
        csvs_nest_catalog=csvs_nest_catalog,
        dump_catalog=dump_catalog
    )


class BinanceDataMerger:

    __slots__ = []

    def __init__(self):
        ...

    def merge(
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
            BinanceDataMerger._get_list_of_asset_parameters_of_files_that_exists_in_specified_directory(
                csvs_nest_catalog
            )
        )

        list_of_asset_parameters_list_to_be_merged = (
            BinanceDataMerger._get_list_of_asset_parameters_list_to_be_merged(
                date_range=date_range,
                pairs=pairs,
                markets=markets,
                stream_types=stream_types,
                should_join_pairs_into_one_csv=should_join_pairs_into_one_csv,
                should_join_markets_into_one_csv=should_join_markets_into_one_csv
            )
        )

        unreachable_csv_asset_parameter_list = []
        for csv in list_of_asset_parameters_list_to_be_merged:
            for asset_parameter in csv:
                if asset_parameter not in existing_asset_parameters_list:
                    unreachable_csv_asset_parameter_list.append(asset_parameter)

        if unreachable_csv_asset_parameter_list:
            missing_csv_str = "\n".join(str(asset_parameter) for asset_parameter in unreachable_csv_asset_parameter_list)
            raise Exception(f"Missing csv of parameters:\n{missing_csv_str}")

            # print(f"Missing csv of parameters:\n{missing_csv_str}")
            #
            # from binance_data_processor.scraper.scraper import download_csv_data
            # import os
            # from dotenv import load_dotenv
            # env_path = os.path.join(os.path.expanduser('~'), 'Documents/env/binance-archiver-ba2-v2-prod.env')
            # load_dotenv(env_path)
            #
            # unique_dates = list(set(str(asset_parameter.date) for asset_parameter in unreachable_csv_asset_parameter_list))
            # unique_pairs = list(set(str(asset_parameter.pairs[0]) for asset_parameter in unreachable_csv_asset_parameter_list))
            # unique_markets = list(set(str(asset_parameter.market.value) for asset_parameter in unreachable_csv_asset_parameter_list))
            # unique_stream_types = list(set(str(asset_parameter.stream_type.value) for asset_parameter in unreachable_csv_asset_parameter_list))
            #
            # if len(unique_dates) == 1:
            #     unique_dates = [unique_dates[0], unique_dates[0]]
            #
            # print(unique_dates)
            # print(unique_pairs)
            # print(unique_markets)
            # print(unique_stream_types)
            #
            # download_csv_data(
            #     date_range=unique_dates,
            #     pairs=unique_pairs,
            #     markets=unique_markets,
            #     stream_types=unique_stream_types,
            #     skip_existing=True,
            #     amount_of_files_to_be_downloaded_at_once=80
            # )

        BinanceDataMerger._main_merge_loop(
            list_of_asset_parameters_list=list_of_asset_parameters_list_to_be_merged,
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
    def _get_list_of_asset_parameters_list_to_be_merged(
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
    def _main_merge_loop(list_of_asset_parameters_list: list[list[AssetParameters]], csvs_nest_catalog: str, dump_catalog: str) -> None:
        for list_of_asset_parameters_for_single_csv in list_of_asset_parameters_list:
            print(f'SINGLE CSV')
            single_csv_df = BinanceDataMerger._single_target_csv_loop(list_of_asset_parameters_for_single_csv, csvs_nest_catalog)

            dataframe_quality_report_list = get_merged_csv_quality_report(
                csvs_nest_catalog=csvs_nest_catalog,
                dataframe=single_csv_df,
                asset_parameters_list=list_of_asset_parameters_for_single_csv
            )

            save_df_with_data_quality_reports(
                dataframe=single_csv_df,
                dataframe_quality_reports=dataframe_quality_report_list,
                dump_catalog=dump_catalog,
                filename=get_base_of_merged_csv_filename(list_of_asset_parameters_for_single_csv)
            )
            print()

    @staticmethod
    def _single_target_csv_loop(list_of_asset_parameters_for_single_csv: list[AssetParameters], csvs_nest_catalog) -> pd.DataFrame:
        dataframes_to_be_merged_within_single_csv = []
        per_market_dict = BinanceDataMerger._group_asset_parameters_by_market(list_of_asset_parameters_for_single_csv)
        for market, params_for_market in per_market_dict.items():
            per_pair_dict = BinanceDataMerger._group_asset_parameters_by_pair(params_for_market)
            pairs_to_be_merged = []
            print(f'         └─single market')
            for pair, params_for_pair in per_pair_dict.items():
                merged_orders_and_trades = BinanceDataMerger._merge_difference_depth_with_depth_snapshot_with_trade_within_single_pair(
                    asset_parameters_list_for_single_market=params_for_pair,
                    csvs_nest_catalog=csvs_nest_catalog
                )
                pairs_to_be_merged.append(merged_orders_and_trades)
            merged_pairs_within_single_market = BinanceDataMerger._merge_pairs_within_single_market(pairs_to_be_merged)
            dataframes_to_be_merged_within_single_csv.append(merged_pairs_within_single_market)
        single_target_csv = BinanceDataMerger._merge_markets_within_target_csv(dataframes_to_be_merged_within_single_csv)

        single_target_csv = BinanceDataMerger._reorder_columns(single_target_csv)
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
    def _merge_difference_depth_with_depth_snapshot_with_trade_within_single_pair(asset_parameters_list_for_single_market: list[AssetParameters], csvs_nest_catalog) -> pd.DataFrame:
        import pandas as pd

        print('                       └─single pair')
        for _ in asset_parameters_list_for_single_market:
            print(f'                                   └─{_}')

        difference_depth_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM), None)
        trade_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.TRADE_STREAM), None)
        depth_snapshot_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DEPTH_SNAPSHOT), None)

        final_orderbook_snapshot_from_cpp_binance_orderbook = BinanceDataMerger._get_final_orderbook_snapshot_from_cpp_binance_orderbook(difference_depth_asset_parameters, csvs_nest_catalog)
        root_depth_snapshot_dataframe = BinanceDataMerger._load_depth_snapshot_root_csv(depth_snapshot_asset_parameters, csvs_nest_catalog)
        root_difference_depth_dataframe = BinanceDataMerger._load_difference_depth_root_csv(difference_depth_asset_parameters, csvs_nest_catalog)
        root_trade_dataframe = BinanceDataMerger._load_trade_root_csv(trade_asset_parameters, csvs_nest_catalog)

        orders_and_trades_df = pd.concat(
            [
                root_difference_depth_dataframe,
                root_trade_dataframe
            ],
            ignore_index=True
        )
        orders_and_trades_df = orders_and_trades_df.sort_values(by=['TimestampOfReceiveUS', 'StreamType', 'ServiceId']).reset_index(drop=True)
        del root_difference_depth_dataframe, root_trade_dataframe

        orders_trades_snapshots = BinanceDataMerger._inject_snapshots_into_orders_and_trades(orders_and_trades_df, root_depth_snapshot_dataframe)
        del orders_and_trades_df, root_depth_snapshot_dataframe

        final_combined_df = pd.concat(
            [
                final_orderbook_snapshot_from_cpp_binance_orderbook,
                orders_trades_snapshots
            ],
            ignore_index=True
        ).reset_index(drop=True)

        del final_orderbook_snapshot_from_cpp_binance_orderbook, orders_trades_snapshots

        final_combined_df['ServiceId'] = range(len(final_combined_df))

        return final_combined_df

    @staticmethod
    def _inject_snapshots_into_orders_and_trades(orders_and_trades_df: pd.DataFrame, root_snapshots_df: pd.DataFrame) -> pd.DataFrame:
        import pandas as pd

        final = orders_and_trades_df.copy()

        for last_id, snap_group in root_snapshots_df.groupby('LastUpdateId'):
            mask = ((final['StreamType'] == 'DIFFERENCE_DEPTH_STREAM') & (final['FinalUpdateId'] > last_id))
            idxs = final[mask].index
            if idxs.empty:
                raise Exception (f'no place to inject snapshot: {last_id}')
                # continue
            insert_idx = idxs[0]

            target_ts = final.iloc[insert_idx]['TimestampOfReceiveUS']
            snap_group = snap_group.copy()
            snap_group['TimestampOfReceiveUS'] = target_ts

            final = pd.concat([
                final.iloc[:insert_idx],
                snap_group,
                final.iloc[insert_idx:]
            ], ignore_index=True)

        return final

    @staticmethod
    def _merge_pairs_within_single_market(list_of_single_pair_dataframe: list[pd.DataFrame]):
        import pandas as pd

        if len(list_of_single_pair_dataframe) == 1:
            return list_of_single_pair_dataframe[0]

        combined_df = pd.concat(list_of_single_pair_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceiveUS', 'Symbol', 'ServiceId'])

        return combined_df

    @staticmethod
    def _merge_markets_within_target_csv(list_of_single_market_dataframe: list[pd.DataFrame]):
        import pandas as pd

        if len(list_of_single_market_dataframe) == 1:
            return list_of_single_market_dataframe[0]

        combined_df = pd.concat(list_of_single_market_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceiveUS', 'Market', 'Symbol', 'ServiceId'])

        return combined_df

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

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f'file {csv_path} - the day before for ob cpp does not exist')

        orderbook_session_simulator = cpp_binance_orderbook.OrderBookSessionSimulator()
        final_orderbook_snapshot = orderbook_session_simulator.compute_final_depth_snapshot(csv_path)

        list_of_entries = []

        for side in (final_orderbook_snapshot.bids(), final_orderbook_snapshot.asks()):
            for entry in side:
                list_of_entries.append(entry.to_list()[:-1])

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
        df['ServiceId'] = range(len(df))
        df['IsLast'] = (df.index == df.index[-1]).astype(int)

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']

        for column in df.columns:
            current_dtype = df[column].dtype
            if is_integer_dtype(current_dtype):
                df[column] = df[column].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[column] = df[column].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[column] = df[column].astype('float64')

        df['TimestampOfReceive'] = df['TimestampOfReceive'].max()
        df['TimestampOfReceiveUS'] = df['TimestampOfReceiveUS'].max()
        return df

    @staticmethod
    def _load_depth_snapshot_root_csv(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        file_path_for_csv = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(file_path_for_csv, comment='#')

        df['StreamType'] = asset_parameter.stream_type.name
        df['Market'] = asset_parameter.market.name
        df['ServiceId'] = range(len(df))
        df['IsLast'] = (df.groupby(['LastUpdateId']).cumcount(ascending=False) == 0).astype(int)

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']

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
        df['ServiceId'] = range(len(df))
        df['IsLast'] = (df.groupby(['FinalUpdateId']).cumcount(ascending=False) == 0).astype(int)

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']


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
        df['ServiceId'] = range(len(df))
        df['IsLast'] = 1

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']


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
    def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:

        desired_order = [
            "TimestampOfReceiveUS",
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
            "PSUnknownField",
            "TradeId",
            "IsBuyerMarketMaker",
            "XUnknownParameter",
            "TimestampOfRequest",
            "MessageOutputTime",
            "LastUpdateId",
            "StreamType",
            "Market",
            "IsLast",
            "ServiceId"
        ]

        existing = [col for col in desired_order if col in df.columns]
        if len(df.columns) != len(df[existing].columns):
            raise Exception(
                f'Columns amount is not the same after reorder '
                f'len(df.columns) != len(df[existing].columns) {len(df.columns)} != {len(df[existing].columns)}'
            )

        return df[existing]
