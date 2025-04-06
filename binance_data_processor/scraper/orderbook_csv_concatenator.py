from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta

from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType


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
    csvs_nest_catalog: str = 'C:/Users/daniel/Documents/binance_archival_data/',
    dump_catalog: str = 'C:/Users/daniel/Documents/sample_merged_csvs_for_ob/'
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
            dump_catalog: str = 'C:/Users/daniel/Documents/sample_merged_csvs_for_ob/'
    ):

        OrderBookConcatenator._prepare_dump_path_catalog(dump_catalog)

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
    def _prepare_dump_path_catalog(dump_path) -> str:
        if not os.path.exists(dump_path):
            os.makedirs(dump_path)
        os.startfile(dump_path)

        return dump_path

    @staticmethod
    def _get_list_of_asset_parameters_of_files_that_exists_in_specified_directory(csv_nest: str) -> list[AssetParameters]:
        local_files = OrderBookConcatenator.list_files_in_specified_directory(csv_nest)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        found_asset_parameter_list = []

        for csv in local_csv_file_paths:
            try:
                asset_parameters = OrderBookConcatenator._decode_asset_parameters_from_csv_name(csv)
                found_asset_parameter_list.append(asset_parameters)
            except Exception as e:
                print(f'_get_existing_files_asset_parameters_list: decode_asset_parameters_from_csv_name sth bad happened: \n {e}')

        return found_asset_parameter_list

    @staticmethod
    def list_files_in_specified_directory(directory_path: str) -> list:
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
    def _get_list_of_asset_parameters_list_to_be_concatenated(
            date_range: list[str],
            pairs: list[str],
            markets: list[Market],
            stream_types: list[StreamType],
            should_join_pairs_into_one_csv: bool = False,
            should_join_markets_into_one_csv: bool = False
    ) -> list[list[AssetParameters]]:

        dates = OrderBookConcatenator._generate_dates_string_list_from_range(date_range)

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
    def _generate_dates_string_list_from_range(date_range: list[str]) -> list[str]:
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

    @staticmethod
    def _main_concatenate_loop(list_of_asset_parameters_list: list[list[AssetParameters]], csvs_nest_catalog: str, dump_catalog: str) -> None:
        for list_of_asset_parameters_for_single_csv in list_of_asset_parameters_list:
            print(f'SINGLE CSV')
            single_target_csv = OrderBookConcatenator._single_target_csv_loop(list_of_asset_parameters_for_single_csv, csvs_nest_catalog)
            single_csv_filename = OrderBookConcatenator._get_single_dataframe_filename(list_of_asset_parameters_for_single_csv)
            single_target_csv.to_csv(f'{dump_catalog}/{single_csv_filename}', index=False)
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
                merged_orders_and_trades = OrderBookConcatenator._concatenate_orders_and_trades_within_single_pair(
                    asset_parameters_list_for_single_market=params_for_pair,
                    csvs_nest_catalog=csvs_nest_catalog
                )
                pairs_to_be_merged.append(merged_orders_and_trades)
            merged_pairs_within_single_market = OrderBookConcatenator._concatenate_pairs_within_single_market(pairs_to_be_merged)
            dataframes_to_be_merged_within_single_csv.append(merged_pairs_within_single_market)

        single_target_csv = OrderBookConcatenator._concatenate_markets_within_single_csv(dataframes_to_be_merged_within_single_csv)
        return single_target_csv

    @staticmethod
    def _concatenate_orders_and_trades_within_single_pair(asset_parameters_list_for_single_market: list[AssetParameters], csvs_nest_catalog) -> pd.DataFrame:
        import pandas as pd

        print('                       └─single pair')
        for _ in asset_parameters_list_for_single_market:
            print(f'                                   └─{_}')

        difference_depth_asset_parameters = next(
            (ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM), None
        )
        trade_asset_parameters = next(
            (ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.TRADE_STREAM), None
        )
        difference_depth_dataframe = OrderBookConcatenator._prepare_single_market_single_pair_trade_or_difference_depth_dataframe(difference_depth_asset_parameters, csvs_nest_catalog)
        trade_dataframe = OrderBookConcatenator._prepare_single_market_single_pair_trade_or_difference_depth_dataframe(trade_asset_parameters, csvs_nest_catalog)
        # file_path_for_orderbook_csv = (
        #         csvs_nest_catalog +
        #         OrderBookConcatenator._get_base_of_filename(difference_depth_asset_parameters) +
        #         '.csv'
        # )
        #
        # difference_depth_columns_to_load = OrderBookConcatenator._get_columns_for_specified_asset_parameters(difference_depth_asset_parameters)
        # difference_depth_dataframe = pd.read_csv(file_path_for_orderbook_csv, comment='#', usecols=difference_depth_columns_to_load)
        # difference_depth_dataframe['IsOrderBookEvent'] = 1
        # difference_depth_dataframe['StreamType'] = StreamType.DIFFERENCE_DEPTH_STREAM.name
        # difference_depth_dataframe['Market'] = difference_depth_asset_parameters.market.name
        # difference_depth_dataframe['aux_idx'] = range(len(difference_depth_dataframe))
        # if difference_depth_asset_parameters.market is not Market.SPOT:
        #     difference_depth_dataframe['TimestampOfReceive'] *= 1000
        #
        # file_path_for_trade_csv = (
        #         csvs_nest_catalog +
        #         OrderBookConcatenator._get_base_of_filename(trade_asset_parameters) +
        #         '.csv'
        # )
        # trade_columns_to_load = OrderBookConcatenator._get_columns_for_specified_asset_parameters(trade_asset_parameters)
        # trade_dataframe = pd.read_csv(file_path_for_trade_csv, comment='#', usecols=trade_columns_to_load)
        # trade_dataframe['IsOrderBookEvent'] = 0
        # trade_dataframe['StreamType'] = StreamType.TRADE_STREAM.name
        # trade_dataframe['Market'] = trade_asset_parameters.market.name
        # trade_dataframe['aux_idx'] = range(len(trade_dataframe))
        # if trade_asset_parameters.market is not Market.SPOT:
        #     trade_dataframe['TimestampOfReceive'] *= 1000
        #
        # for df in [difference_depth_dataframe, trade_dataframe]:
        #     for column in df.columns:
        #         current_dtype = df[column].dtype
        #         if is_integer_dtype(current_dtype):
        #             df[column] = df[column].astype('Int64')
        #         elif is_bool_dtype(current_dtype):
        #             df[column] = df[column].astype('boolean')
        #         elif is_float_dtype(current_dtype):
        #             df[column] = df[column].astype('float64')

        combined_df = pd.concat([difference_depth_dataframe, trade_dataframe], ignore_index=True)

        combined_df = combined_df.sort_values(by=['TimestampOfReceive', 'IsOrderBookEvent', 'aux_idx'])
        # combined_df.drop(columns=['aux_idx'])

        # combined_df.to_csv(f'combined_{asset_parameters_list_for_single_market[0].market.name}_{asset_parameters_list_for_single_market[0].date}.csv', index=False)
        return combined_df

    @staticmethod
    def _prepare_single_market_single_pair_trade_or_difference_depth_dataframe(asset_parameter: AssetParameters, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        file_path_for_csv = f'{csvs_nest_catalog}/{OrderBookConcatenator._get_base_of_filename(asset_parameter)}.csv'
        columns_to_load = OrderBookConcatenator._get_columns_for_specified_asset_parameters(asset_parameter)

        df = pd.read_csv(file_path_for_csv, comment='#', usecols=columns_to_load)
        df['IsOrderBookEvent'] = 0 if asset_parameter.stream_type is StreamType.TRADE_STREAM else 1
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

        if len(list_of_single_pair_dataframe) == 0:
            return list_of_single_pair_dataframe[0]

        combined_df = pd.concat(list_of_single_pair_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceive', 'Symbol', 'IsOrderBookEvent', 'aux_idx'])
        return combined_df

    @staticmethod
    def _concatenate_markets_within_single_csv(list_of_single_market_dataframe: list[pd.DataFrame]):
        import pandas as pd

        if len(list_of_single_market_dataframe) == 0:
            return list_of_single_market_dataframe[0]

        combined_df = pd.concat(list_of_single_market_dataframe, ignore_index=True)
        combined_df = combined_df.sort_values(by=['TimestampOfReceive', 'Market', 'Symbol', 'IsOrderBookEvent', 'aux_idx'])
        return combined_df

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
    def _get_single_dataframe_filename(list_of_asset_parameters_for_single_csv: list[AssetParameters]) -> str:
        streams = sorted({ap.stream_type.name.lower() for ap in list_of_asset_parameters_for_single_csv})
        markets = sorted({ap.market.name.lower() for ap in list_of_asset_parameters_for_single_csv})
        pairs = sorted({ap.pairs[0].lower() for ap in list_of_asset_parameters_for_single_csv})
        date = list_of_asset_parameters_for_single_csv[0].date
        return f"merged_{'_'.join(streams)}_{'_'.join(markets)}_{'_'.join(pairs)}_{date}.csv"

    @staticmethod
    def _get_columns_for_specified_asset_parameters(asset_parameters: AssetParameters) -> list[str]:
        mapping = {
            StreamType.DEPTH_SNAPSHOT: {
                # market: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity', 'LastUpdateId']
                market: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity']
                for market in (Market.SPOT, Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
            },
            StreamType.DIFFERENCE_DEPTH_STREAM: {
                # Market.SPOT: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity', 'FirstUpdateId', 'FinalUpdateId'],
                Market.SPOT: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity'],
                **{
                    # market: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity', 'FirstUpdateId', 'FinalUpdateId', 'FinalUpdateIdInLastStream']
                    market: ['TimestampOfReceive', 'Symbol', 'IsAsk', 'Price', 'Quantity']
                    for market in (Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
                }
            },
            StreamType.TRADE_STREAM: {
                # market: ['TimestampOfReceive', 'Symbol', 'IsBuyerMarketMaker', 'Price', 'Quantity', 'TradeId']
                market: ['TimestampOfReceive', 'Symbol', 'IsBuyerMarketMaker', 'Price', 'Quantity']
                for market in (Market.SPOT, Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
            }
        }

        return mapping[asset_parameters.stream_type][asset_parameters.market]

    @staticmethod
    def _get_columns_dtypes_for_specified_asset_parameters(asset_parameters: AssetParameters) -> {str}:
        mapping = {
            StreamType.DEPTH_SNAPSHOT: {
                market: {'TimestampOfReceive': int, 'IsAsk': bool, 'Price':float, 'Quantity':float, 'LastUpdateId': int}
                for market in (Market.SPOT, Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
            },
            StreamType.DIFFERENCE_DEPTH_STREAM: {
                Market.SPOT: {'TimestampOfReceive': int, 'IsAsk': bool, 'Price':float, 'Quantity':float, 'FirstUpdateId':int, 'FinalUpdateId': int},
                **{
                    market: {'TimestampOfReceive': int, 'IsAsk': bool, 'Price':float, 'Quantity':float, 'FirstUpdateId':int, 'FinalUpdateId': int, 'FinalUpdateIdInLastStream': int}
                    for market in (Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
                }
            },
            StreamType.TRADE_STREAM: {
                market: {'TimestampOfReceive': int, 'IsBuyerMarketMaker': bool, 'Price': float, 'Quantity': float, 'TradeId': int}
                for market in (Market.SPOT, Market.USD_M_FUTURES, Market.COIN_M_FUTURES)
            }
        }

        return mapping[asset_parameters.stream_type][asset_parameters.market]

    @staticmethod
    def _get_base_of_filename(asset_parameters: AssetParameters) -> str:
        return f"binance_{asset_parameters.stream_type.name.lower()}_{asset_parameters.market.name.lower()}_{asset_parameters.pairs[0].lower()}_{asset_parameters.date}"
