from __future__ import annotations

from collections import defaultdict
import os

from binance_data_processor import download_csv_data
from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.scraper.concatenator_config import DataConcatenatorConfig
from binance_data_processor.scraper.scraper_config import ScraperCfg
from binance_data_processor.data_quality.data_quality_checker import get_merged_csv_quality_reports
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.market_enum import Market, M_STREAM_CODE
from binance_data_processor.enums.stream_type_enum import StreamType, ST_STREAM_CODE
from binance_data_processor.enums.symbol_enum import symbol_to_int
from binance_data_processor.utils.file_utils import get_base_of_root_csv_filename
from binance_data_processor.utils.file_utils import get_list_of_existing_merged_csvs_asset_parameters
from binance_data_processor.utils.file_utils import get_base_of_merged_csv_filename
from binance_data_processor.utils.file_utils import to_csv_w_data_quality_reports
from binance_data_processor.utils.time_utils import gen_dates
from binance_data_processor.utils.time_utils import get_yesterday_date

__all__ = [
    'make_merged_csvs'
]

DESIRED_COLS_ORDER = [
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
    "MUnknownParameter",
    "XUnknownParameter",
    "Pair",
    "TimestampOfRequest",
    "MessageOutputTime",
    "LastUpdateId",
    "StreamType",
    "Market",
    "IsLast",
    "ServiceId"
]


COLS_TO_DROP = [
    'TimestampOfReceive',
    'ServiceId',
    'Stream',
    'EventType',
    'EventTime',
    'TransactionTime',
    'TradeId',
    'FirstUpdateId',
    'FinalUpdateId',
    'FinalUpdateIdInLastStream',
    'PSUnknownField',
    'TimestampOfRequest',
    'MessageOutputTime',
    'LastUpdateId',
    'MUnknownParameter',
    'XUnknownParameter',
    'Pair',
]


def make_merged_csvs(cfg: DataConcatenatorConfig) -> None:
    binance_data_merger = BinanceDataMerger()
    binance_data_merger.merge(cfg)


class BinanceDataMerger:
    __slots__ = []

    def __init__(self):
        ...

    @staticmethod
    def merge(cfg: DataConcatenatorConfig):

        print(f'\033[35m{binance_archiver_logo}\033[0m')

        list_of_asset_parameters_list = BinanceDataMerger._resolve_list_of_asset_parameters_list(cfg)

        BinanceDataMerger._download_missing_root_csvs(cfg)

        BinanceDataMerger._main_merge_loop(
            list_of_asset_parameters_list=list_of_asset_parameters_list,
            csvs_nest_catalog=cfg.csvs_nest_catalog,
            dump_catalog=cfg.dump_path
        )

    @staticmethod
    def _resolve_list_of_asset_parameters_list(cfg: DataConcatenatorConfig) -> list[list[AssetParams]]:
        to_merge = BinanceDataMerger._get_list_of_asset_parameters_list_to_be_merged(
            date_range=cfg.date_range,
            pairs=cfg.pairs,
            markets=cfg.markets,
            stream_types=cfg.stream_types,
            join_pairs_into_one_csv=cfg.join_pairs_into_one_csv,
            join_markets_into_one_csv=cfg.join_markets_into_one_csv
        )

        if not cfg.skip_existing:
            return to_merge

        existing = get_list_of_existing_merged_csvs_asset_parameters(cfg.dump_path)

        def canonical(group: list[AssetParams]) -> list[str]:
            return sorted(str(ap) for ap in group)

        existing_canon = {tuple(canonical(g)) for g in existing}

        result = []
        for group in to_merge:
            key = tuple(canonical(group))
            if key not in existing_canon:
                result.append(group)

        return result

    @staticmethod
    def _get_list_of_asset_parameters_list_to_be_merged(
            date_range: list[str],
            pairs: list[str],
            markets: list[Market],
            stream_types: list[StreamType],
            join_pairs_into_one_csv: bool = False,
            join_markets_into_one_csv: bool = False
    ) -> list[list[AssetParams]]:

        dates = gen_dates(date_range)

        groups = []

        # Case 1: Do not join pairs and do not join markets – group by (pair, market, date)
        if not join_pairs_into_one_csv and not join_markets_into_one_csv:
            for pair in pairs:
                pair = pair.lower()
                for market in markets:
                    for date in dates:
                        group = []
                        for stream in stream_types:
                            asset_param = AssetParams(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                        groups.append(group)

        # Case 2: Do not join pairs, join markets – group by (pair, date),
        elif not join_pairs_into_one_csv and join_markets_into_one_csv:
            for pair in pairs:
                pair = pair.lower()
                for date in dates:
                    group = []
                    for market in markets:
                        for stream in stream_types:
                            asset_param = AssetParams(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                    groups.append(group)

        # Case 3: Join pairs, do not join markets – group by (market, date),
        elif join_pairs_into_one_csv and not join_markets_into_one_csv:
            for market in markets:
                for date in dates:
                    group = []
                    for pair in pairs:
                        pair = pair.lower()
                        for stream in stream_types:
                            asset_param = AssetParams(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                    groups.append(group)

        # Case 4: Join pairs and join markets – group by date only,
        else:  # should_join_pairs_into_one_csv and should_join_markets_into_one_csv
            for date in dates:
                group = []
                for pair in pairs:
                    pair = pair.lower()
                    for market in markets:
                        for stream in stream_types:
                            asset_param = AssetParams(
                                market=Market(market),
                                stream_type=StreamType(stream),
                                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                                date=date
                            )
                            group.append(asset_param)
                groups.append(group)

        return groups

    @staticmethod
    def _download_missing_root_csvs(cfg: DataConcatenatorConfig) -> None:

        date_from = get_yesterday_date(cfg.date_range[0])
        date_to = cfg.date_range[1]

        scraper_cfg = ScraperCfg(
            date_range=[date_from, date_to],
            pairs=cfg.pairs,
            markets=cfg.markets,
            stream_types=cfg.stream_types,
            dump_path=cfg.csvs_nest_catalog
        )
        download_csv_data(scraper_cfg)

    @staticmethod
    def _main_merge_loop(list_of_asset_parameters_list: list[list[AssetParams]], csvs_nest_catalog: str, dump_catalog: str) -> None:
        from alive_progress import alive_bar
        print(f'\033[36mought to concatenate {len(list_of_asset_parameters_list)} file(s):\n')

        with alive_bar(
                len(list_of_asset_parameters_list),
                force_tty=True,
                spinner='dots_waves',
                title='',
                enrich_print=False,
                stats=False,
                dual_line=False
        ) as bar:
            for csv_ap_list in list_of_asset_parameters_list:
                print(f'SINGLE CSV')
                df = BinanceDataMerger._target_csv_loop(csv_ap_list, csvs_nest_catalog)

                reports = get_merged_csv_quality_reports(
                    csvs_nest_catalog=csvs_nest_catalog,
                    df=df,
                    asset_parameters_list=csv_ap_list
                )

                BinanceDataMerger._drop_non_standard_x_and_m_unknown_parameters(df)
                BinanceDataMerger.drop_redundant_columns(df)
                BinanceDataMerger.to_int(df)

                to_csv_w_data_quality_reports(
                    df=df,
                    data_quality_reports=reports,
                    dump_catalog=dump_catalog,
                    filename=get_base_of_merged_csv_filename(csv_ap_list)
                )
                bar()

        print(f'\033[0m')

    @staticmethod
    def _target_csv_loop(list_of_asset_parameters_for_single_csv: list[AssetParams], csvs_nest_catalog) -> pd.DataFrame:
        dfs_to_be_merged_within_single_csv = []
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
            dfs_to_be_merged_within_single_csv.append(merged_pairs_within_single_market)
        single_target_csv_df = BinanceDataMerger._merge_markets_within_target_csv(dfs_to_be_merged_within_single_csv)

        single_target_csv_df = BinanceDataMerger._reorder_cols(single_target_csv_df)
        return single_target_csv_df

    @staticmethod
    def _group_asset_parameters_by_market(asset_parameters: list[AssetParams]) -> dict[Market, list[AssetParams]]:
        grouped: dict[Market, list[AssetParams]] = defaultdict(list)
        for ap in asset_parameters:
            grouped[ap.market].append(ap)
        return dict(grouped)

    @staticmethod
    def _group_asset_parameters_by_pair(asset_parameters: list[AssetParams]) -> dict[str, list[AssetParams]]:
        grouped: dict[str, list[AssetParams]] = defaultdict(list)
        for ap in asset_parameters:
            grouped[ap.pairs[0]].append(ap)
        return dict(grouped)

    @staticmethod
    def _merge_difference_depth_with_depth_snapshot_with_trade_within_single_pair(asset_parameters_list_for_single_market: list[AssetParams], csvs_nest_catalog) -> pd.DataFrame:
        import pandas as pd

        print('                       └─single pair')
        for _ in asset_parameters_list_for_single_market:
            print(f'                                   └─{_}')
        print()

        difference_depth_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DIFFERENCE_DEPTH_STREAM), None)
        trade_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.TRADE_STREAM), None)
        depth_snapshot_asset_parameters = next((ap for ap in asset_parameters_list_for_single_market if ap.stream_type == StreamType.DEPTH_SNAPSHOT), None)

        final_orderbook_snapshot_from_cpp_binance_orderbook = BinanceDataMerger._get_final_depth_snapshot_from_cpp_binance_orderbook(difference_depth_asset_parameters, csvs_nest_catalog)
        root_depth_snapshot_df = BinanceDataMerger._load_depth_snapshot_root_csv(depth_snapshot_asset_parameters, csvs_nest_catalog)
        root_difference_depth_df = BinanceDataMerger._load_difference_depth_root_csv(difference_depth_asset_parameters, csvs_nest_catalog)
        root_trade_df = BinanceDataMerger._load_trade_root_csv(trade_asset_parameters, csvs_nest_catalog)

        orders_and_trades_df = pd.concat([root_difference_depth_df, root_trade_df], ignore_index=True)
        orders_and_trades_df = orders_and_trades_df.sort_values(by=['TimestampOfReceiveUS', 'StreamType', 'ServiceId']).reset_index(drop=True)
        del root_difference_depth_df, root_trade_df

        orders_trades_snapshots = BinanceDataMerger._inject_snapshots_into_orders_and_trades(orders_and_trades_df, root_depth_snapshot_df)
        del orders_and_trades_df, root_depth_snapshot_df

        final_combined_df = pd.concat(
            [final_orderbook_snapshot_from_cpp_binance_orderbook, orders_trades_snapshots],
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
    def _merge_pairs_within_single_market(list_of_single_pair_df: list[pd.DataFrame]):
        import pandas as pd

        if len(list_of_single_pair_df) == 1:
            return list_of_single_pair_df[0]

        comb = pd.concat(list_of_single_pair_df, ignore_index=True)
        comb = comb.sort_values(by=['TimestampOfReceiveUS', 'Symbol', 'ServiceId'])

        return comb

    @staticmethod
    def _merge_markets_within_target_csv(list_of_single_market_dfs: list[pd.DataFrame]):
        import pandas as pd

        if len(list_of_single_market_dfs) == 1:
            return list_of_single_market_dfs[0]

        comb = pd.concat(list_of_single_market_dfs, ignore_index=True)
        comb = comb.sort_values(by=['TimestampOfReceiveUS', 'Market', 'Symbol', 'ServiceId'])

        return comb

    @staticmethod
    def _get_final_depth_snapshot_from_cpp_binance_orderbook(asset_parameter: AssetParams, csvs_nest_catalog: str) -> pd.DataFrame:
        from cpp_binance_orderbook import compute_final_ob

        prev_day_ap = AssetParams(
            market=asset_parameter.market,
            stream_type=asset_parameter.stream_type,
            pairs=asset_parameter.pairs,
            date=get_yesterday_date(asset_parameter.date)
        )

        csv_path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(prev_day_ap)}.csv'

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f'[final snapsoht] {csv_path} - day before does not exist')

        final_ob = compute_final_ob(csv_path)
        df = BinanceDataMerger._cpp_ob_to_df(final_ob)

        df['Symbol'] = df['Symbol'].apply(lambda s: s.name).astype("string")
        df['StreamType'] = 'FINAL_DEPTH_SNAPSHOT'
        df['Market'] = asset_parameter.market.name
        df['ServiceId'] = range(len(df))
        df['IsLast'] = (df.index == df.index[-1]).astype(int)
        df['IsAsk'] = df['IsAsk'].astype('int8')

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']

        df = BinanceDataMerger._standardize_column_dtypes(df)

        if df.shape[0] < 100 or df.shape[1] < 10:
            raise Exception('Insufficient df shape of final depth snapshot:  cpp_binance_orderbook')

        return df

    @staticmethod
    def _load_depth_snapshot_root_csv(asset_parameter: AssetParams, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd

        file_path_for_csv = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(file_path_for_csv, comment='#')

        df = df[df['TimestampOfReceive'] == df['TimestampOfReceive'].iloc[0]]

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

        df = BinanceDataMerger._standardize_column_dtypes(df)

        return df

    @staticmethod
    def _load_difference_depth_root_csv(asset_parameter: AssetParams, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd

        path = f'{csvs_nest_catalog}/{get_base_of_root_csv_filename(asset_parameter)}.csv'
        df = pd.read_csv(path, comment='#')
        df['StreamType'] = asset_parameter.stream_type.name
        df['Market'] = asset_parameter.market.name
        df['ServiceId'] = range(len(df))
        df['IsLast'] = (df.groupby(['FinalUpdateId']).cumcount(ascending=False) == 0).astype(int)

        if asset_parameter.market is not Market.SPOT:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive'] * 1000
        else:
            df['TimestampOfReceiveUS'] = df['TimestampOfReceive']

        df = BinanceDataMerger._standardize_column_dtypes(df)

        return df

    @staticmethod
    def _load_trade_root_csv(asset_parameter: AssetParams, csvs_nest_catalog: str) -> pd.DataFrame:
        import pandas as pd

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

        df = BinanceDataMerger._standardize_column_dtypes(df)

        return df

    @staticmethod
    def _cpp_ob_to_df(ob: cpp_binance_orderbook.OrderBook) -> pd.DataFrame:
        import pandas as pd
        rows = ((e.timestamp_of_receive, e.symbol, e.is_ask, e.price, e.quantity) for e in (ob.bids() + ob.asks()))
        return pd.DataFrame.from_records(rows, columns=["TimestampOfReceive", "Symbol", "IsAsk", "Price", "Quantity"])

    @staticmethod
    def _standardize_column_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        from pandas.core.dtypes.common import is_integer_dtype, is_bool_dtype, is_float_dtype

        for col in df.columns:
            current_dtype = df[col].dtype
            if is_integer_dtype(current_dtype):
                df[col] = df[col].astype('Int64')
            elif is_bool_dtype(current_dtype):
                df[col] = df[col].astype('boolean')
            elif is_float_dtype(current_dtype):
                df[col] = df[col].astype('float64')

        return df

    @staticmethod
    def _reorder_cols(df: pd.DataFrame) -> pd.DataFrame:
        existing = [col for col in DESIRED_COLS_ORDER if col in df.columns]
        if len(df.columns) != len(df[existing].columns):
            raise Exception(
                f'Columns amount is not the same after reorder '
                f'len(df.columns) != len(df[existing].columns) {len(df.columns)} != {len(df[existing].columns)}'
                f'{df.columns}'
                f''
                f'{df[existing].columns}'
            )

        return df[existing]

    @staticmethod
    def _drop_non_standard_x_and_m_unknown_parameters(df: pd.DataFrame) -> pd.DataFrame:
        original_df_len = df.shape[0]
        m_unknown_parameter_len = 0
        x_unknown_parameter_len = 0

        if 'MUnknownParameter' in df.columns:
            m_unknown_parameter_mask = (
                (df['StreamType'] == StreamType.TRADE_STREAM.name) &
                (df['Market'] == Market.SPOT.name) &
                (df['MUnknownParameter'] != True)
            )
            m_unknown_parameter_len = df[m_unknown_parameter_mask].shape[0]
            if True in df[m_unknown_parameter_mask]['MUnknownParameter'].unique():
                raise Exception(f"True in df[x_unknown_parameter_mask]['MUnknownParameter'].unique()")
            df.drop(index=df.index[m_unknown_parameter_mask], inplace=True)

        if 'XUnknownParameter' in df.columns:
            x_unknown_parameter_mask = (
                (df['StreamType'] == StreamType.TRADE_STREAM.name) &
                (df['Market'].isin([Market.USD_M_FUTURES.name, Market.COIN_M_FUTURES.name])) &
                (df['XUnknownParameter'] != 'MARKET')
            )
            x_unknown_parameter_len = df[x_unknown_parameter_mask].shape[0]
            if 'MARKET' in df[x_unknown_parameter_mask]['XUnknownParameter'].unique():
                raise Exception(f"'MARKET' in df[x_unknown_parameter_mask]['XUnknownParameter'].unique()")
            df.drop(index=df.index[x_unknown_parameter_mask], inplace=True)
        df.reset_index(drop=True, inplace=True)
        after_drop_df_len = df.shape[0]

        print(f'Dropping m/x unknowns, original size: {original_df_len}. Dropped: {m_unknown_parameter_len + x_unknown_parameter_len}. After drop: {after_drop_df_len}')
        if after_drop_df_len != original_df_len - m_unknown_parameter_len - x_unknown_parameter_len:
            raise Exception('df len bad after dropping x and m unknown parameters. Presumably dropped too much')

        if 'MUnknownParameter' in df.columns:
            if set(
                    df[
                        (df['StreamType'] == StreamType.TRADE_STREAM.name) &
                        (df['Market'] == Market.SPOT.name)
                    ]['MUnknownParameter'].unique()) != {True}:
                raise Exception(f"MUnknownParameter Uniques after drop: {df[df['StreamType'] == StreamType.TRADE_STREAM.name]['MUnknownParameter'].unique()}")

        if 'XUnknownParameter' in df.columns:
            if set(
                    df[
                        (df['StreamType'] == StreamType.TRADE_STREAM.name) &
                        (df['Market'].isin([Market.USD_M_FUTURES.name, Market.COIN_M_FUTURES.name]))
                    ]['XUnknownParameter'].unique()) != {'MARKET'}:
                raise Exception(f"XUnknownParameter Uniques after drop: {df[df['StreamType'] == StreamType.TRADE_STREAM.name]['XUnknownParameter'].unique()}")

        return df

    @staticmethod
    def drop_redundant_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=COLS_TO_DROP, errors="ignore", inplace=True)

    @staticmethod
    def to_int(df: pd.DataFrame):
        df['Symbol'] = df['Symbol'].apply(symbol_to_int)
        df['Market'] = df['Market'].apply(lambda m: M_STREAM_CODE[Market[m]])
        df['StreamType'] = df['StreamType'].apply(lambda st: ST_STREAM_CODE[StreamType[st]])
        return df
    