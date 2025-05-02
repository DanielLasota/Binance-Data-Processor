from __future__ import annotations

import copy
import io
import os
import queue
import threading
import zipfile
from datetime import timedelta, datetime
import orjson
from abc import ABC, abstractmethod
import time

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.epoch_time_unit import EpochTimeUnit
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.enums.stream_type_enum import StreamType
from binance_data_processor.enums.storage_connection_parameters import StorageConnectionParameters
from binance_data_processor.data_quality.data_quality_checker import get_dataframe_quality_report
from binance_data_processor.utils.file_utils import (
    prepare_dump_path_catalog,
    list_files_in_specified_directory,
    decode_asset_parameters_from_csv_name,
    get_base_of_root_csv_filename, save_df_with_data_quality_reports
)
from binance_data_processor.utils.time_utils import generate_dates_string_list_from_range


__all__ = [
    'download_csv_data'
]


def download_csv_data(
        date_range: list[str],
        pairs: list[str],
        markets: list[str],
        stream_types: list[str],
        skip_existing: bool = True,
        amount_of_files_to_be_downloaded_at_once: int = 10,
        dump_path: str | None = None
) -> None:

    data_scraper = DataScraper()

    data_scraper.run(
        markets=markets,
        stream_types=stream_types,
        pairs=pairs,
        date_range=date_range,
        dump_path=dump_path,
        skip_existing=skip_existing,
        amount_of_files_to_be_downloaded_at_once=amount_of_files_to_be_downloaded_at_once
    )


class DataScraper:

    __slots__ = [
        'storage_client',
        'amount_of_files_to_be_downloaded_at_once'
    ]

    def __init__(self) -> None:
        storage_connection_parameters = StorageConnectionParameters()
        if storage_connection_parameters.azure_blob_parameters_with_key is not None:
            self.storage_client = AzureClient(
                blob_connection_string=storage_connection_parameters.azure_blob_parameters_with_key,
                container_name=storage_connection_parameters.azure_container_name
            )
        elif storage_connection_parameters.backblaze_access_key_id is not None:
            self.storage_client = BackBlazeS3Client(
                access_key_id=storage_connection_parameters.backblaze_access_key_id,
                secret_access_key=storage_connection_parameters.backblaze_secret_access_key,
                endpoint_url=storage_connection_parameters.backblaze_endpoint_url,
                bucket_name=storage_connection_parameters.backblaze_bucket_name
            )
        else:
            raise ValueError('No storage specified...')

        self.amount_of_files_to_be_downloaded_at_once = ...

    def run(
            self,
            markets: list[str],
            stream_types: list[str],
            pairs: list[str],
            date_range: list[str],
            dump_path: str | None = None,
            skip_existing: bool = True,
            amount_of_files_to_be_downloaded_at_once: int = 10
    ) -> None:
        print(f'\033[35m{binance_archiver_logo}')

        self.amount_of_files_to_be_downloaded_at_once = amount_of_files_to_be_downloaded_at_once

        default_dump_path = os.path.join(os.path.expanduser("~"), "Documents", "binance_archival_data").replace('\\', '/')
        dump_path = dump_path or default_dump_path
        prepare_dump_path_catalog(dump_path)

        dates_to_be_downloaded = generate_dates_string_list_from_range(date_range)

        asset_parameters_to_be_downloaded = self._get_asset_parameters_to_be_downloaded(
            markets=markets,
            stream_types=stream_types,
            pairs=pairs,
            dates_to_be_downloaded=dates_to_be_downloaded,
            dump_path=dump_path,
            skip_existing=skip_existing
        )
        amount_of_files_to_be_made = len(asset_parameters_to_be_downloaded)

        print(f'\033[36m\n ought to download {amount_of_files_to_be_made} file(s)\n')

        data_quality_report_list = []

        self._main_download_loop(
            asset_parameters_list=asset_parameters_to_be_downloaded,
            data_quality_report_list=data_quality_report_list,
            dump_path=dump_path
        )

        if len(data_quality_report_list) == 0:
            return

        print(f'\nFinished: {dump_path} \n')

        data_quality_positive_report_list = [
            report for report
            in data_quality_report_list
            if report.is_data_quality_report_positive()
        ]

        positive_reports_percentage = len(data_quality_positive_report_list) / len(data_quality_report_list) * 100
        print(f'Positive/All: {len(data_quality_positive_report_list)}/{len(data_quality_report_list)} ({positive_reports_percentage}%)')

        i = 1
        for report in data_quality_report_list:
            print(f'{i}. {report.asset_parameters}: {report.get_data_report_status()}')
            i += 1

        while True:
            try:
                user_input = input('\n(n/exit): ').strip().lower()
                if user_input == 'exit':
                    break
                print(data_quality_report_list[int(user_input) - 1])
            except Exception as e:
                print(e)

    def _get_asset_parameters_to_be_downloaded(self, markets: list[str], stream_types: list[str], pairs: list[str], dates_to_be_downloaded: list[str], dump_path: str, skip_existing: bool) -> list[AssetParameters]:
        asset_parameters_to_be_downloaded = self._generate_asset_parameters_list_to_be_downloaded(
            markets=markets,
            stream_types=stream_types,
            pairs=pairs,
            dates=dates_to_be_downloaded
        )

        if skip_existing is True:
            asset_parameters_of_existing_files = self._get_existing_in_nest_files_catalog_asset_parameters_list(csv_nest=dump_path)
            asset_parameters_to_be_downloaded_minus_existing_files_asset_parameters = [
                asset for asset
                in asset_parameters_to_be_downloaded
                if asset not in asset_parameters_of_existing_files
            ]
            asset_parameters_to_be_downloaded_that_already_exists = [
                asset for asset
                in asset_parameters_to_be_downloaded
                if asset in asset_parameters_of_existing_files
            ]
            if len(asset_parameters_to_be_downloaded_that_already_exists) > 0:
                print(f'skip_existing = True ==> skipping existing assets:')
                for asset in asset_parameters_to_be_downloaded_that_already_exists:
                    print(f'    └─{asset}')
        else:
            asset_parameters_to_be_downloaded_minus_existing_files_asset_parameters = asset_parameters_to_be_downloaded

        return asset_parameters_to_be_downloaded_minus_existing_files_asset_parameters

    @staticmethod
    def _get_existing_in_nest_files_catalog_asset_parameters_list(csv_nest: str) -> list[AssetParameters]:
        local_files = list_files_in_specified_directory(csv_nest)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        print(f"Found {len(local_csv_file_paths)} CSV files out of {len(local_files)} total files\n")

        found_asset_parameter_list = []

        for csv in local_csv_file_paths:
            try:
                asset_parameters = decode_asset_parameters_from_csv_name(csv)
                found_asset_parameter_list.append(asset_parameters)
            except Exception as e:
                print(f'_get_existing_files_asset_parameters_list: decode_asset_parameters_from_csv_name sth bad happened: \n {e}')

        return found_asset_parameter_list

    @staticmethod
    def _generate_asset_parameters_list_to_be_downloaded(markets: list[str], stream_types: list[str], pairs: list[str], dates: list[str]) -> list[AssetParameters]:
        markets = [Market(market.lower()) for market in markets]
        stream_types = [StreamType(stream_type.lower()) for stream_type in stream_types]
        pairs = [pair.lower() for pair in pairs]

        return [
            AssetParameters(
                market=market,
                stream_type=stream_type,
                pairs=[(f'{pair[:-1]}_perp' if market == Market.COIN_M_FUTURES else pair)],
                date=date
            )
            for market in markets
            for stream_type in stream_types
            for pair in pairs
            for date in dates
        ]

    def _main_download_loop(self, asset_parameters_list: list[AssetParameters], dump_path: str, data_quality_report_list: list) -> None:

        for asset_parameters in asset_parameters_list:
            print(f'Downloading: {asset_parameters}')
            dataframe = self._get_rough_dataframe_from_cloud_storage_files_cut_to_specified_date(asset_parameters)
            dataframe_quality_report = get_dataframe_quality_report(
                dataframe=dataframe,
                asset_parameters=asset_parameters,
            )
            print(f'data report status: {dataframe_quality_report.get_data_report_status().value}')
            print()
            data_quality_report_list.append(dataframe_quality_report)

            # minimal_dataframe = self._get_minimal_dataframe(df=rough_dataframe_for_quality_check)

            target_file_name = get_base_of_root_csv_filename(asset_parameters=asset_parameters)

            save_df_with_data_quality_reports(
                dataframe=dataframe,
                dataframe_quality_reports=dataframe_quality_report,
                dump_catalog=dump_path,
                filename=target_file_name
            )

            del dataframe_quality_report
            del dataframe

    def _get_rough_dataframe_from_cloud_storage_files_cut_to_specified_date(self, asset_parameters: AssetParameters) -> pd.DataFrame:
        list_of_files_with_specified_prefixes_that_should_be_downloaded = (
            self._get_prefix_of_files_that_should_be_downloaded_for_specified_single_date(asset_parameters=asset_parameters)
        )

        list_of_files_to_be_downloaded = self.storage_client.list_files_with_prefixes(list_of_files_with_specified_prefixes_that_should_be_downloaded)

        stream_type_download_handler = self._get_stream_type_download_handler(asset_parameters.stream_type)
        dataframe = stream_type_download_handler(list_of_files_to_be_downloaded, asset_parameters.market)

        epoch_time_unit = (
            EpochTimeUnit.MICROSECONDS
            if asset_parameters.market is Market.SPOT and asset_parameters.stream_type is not StreamType.DEPTH_SNAPSHOT
            else EpochTimeUnit.MILLISECONDS
        )

        dataframe = self._cut_dataframe_to_the_range_of_single_day(
            dataframe=dataframe,
            target_day=asset_parameters.date,
            epoch_time_unit=epoch_time_unit
        )

        return dataframe

    @staticmethod
    def _cut_dataframe_to_the_range_of_single_day(dataframe: pd.DataFrame, target_day: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> pd.DataFrame:
        import pandas as pd

        date_obj = datetime.strptime(target_day, '%d-%m-%Y')
        start_time = pd.Timestamp(date_obj, tz='UTC')

        start_epoch = int(start_time.timestamp() * epoch_time_unit.multiplier_of_second)
        end_time = start_time + pd.Timedelta(days=1) - pd.Timedelta(value=1, unit=epoch_time_unit.value)
        end_epoch = int(end_time.timestamp() * epoch_time_unit.multiplier_of_second)

        return dataframe[
            (dataframe['TimestampOfReceive'] >= start_epoch) &
            (dataframe['TimestampOfReceive'] <= end_epoch)
        ]

    @staticmethod
    def _get_prefix_of_files_that_should_be_downloaded_for_specified_single_date(asset_parameters: AssetParameters) -> list[str]:
        one_date_after = (datetime.strptime(asset_parameters.date, '%d-%m-%Y') + timedelta(days=1)).strftime('%d-%m-%Y')

        asset_parameters_for_the_next_day = copy.copy(asset_parameters)
        asset_parameters_for_the_next_day.date = one_date_after

        target_day_date_prefix = get_base_of_root_csv_filename(asset_parameters=asset_parameters)
        day_date_one_after_prefix = get_base_of_root_csv_filename(asset_parameters=asset_parameters_for_the_next_day) + 'T00-0'

        return [target_day_date_prefix, day_date_one_after_prefix]

    def _get_stream_type_download_handler(self, stream_type: StreamType) -> callable:
        handler_lookup = {
            StreamType.DIFFERENCE_DEPTH_STREAM: self._difference_depth_stream_download_handler,
            StreamType.TRADE_STREAM: self._trade_stream_type_download_handler,
            StreamType.DEPTH_SNAPSHOT: self._depth_snapshot_stream_type_download_handler
        }
        return handler_lookup[stream_type]

    def _difference_depth_stream_download_handler(self, list_of_file_to_be_downloaded: list[str], market: Market) -> pd.DataFrame:
        import pandas as pd
        from alive_progress import alive_bar

        len_of_files_to_be_downloaded = len(list_of_file_to_be_downloaded)

        records = []
        with alive_bar(len_of_files_to_be_downloaded, force_tty=True, spinner='dots_waves', title='') as bar:
            for i in range(0, len_of_files_to_be_downloaded, self.amount_of_files_to_be_downloaded_at_once):
                batch = list_of_file_to_be_downloaded[i:i + self.amount_of_files_to_be_downloaded_at_once]
                result_queue = queue.Queue()
                threads_list = []

                for idx, file_name in enumerate(batch, start=i):
                    thread = threading.Thread(target=self.download_file_and_put_json_into_queue, args=(file_name, result_queue, idx))
                    threads_list.append(thread)
                    thread.start()

                for thread in threads_list:
                    thread.join()

                batch_results = [result_queue.get() for _ in range(len(batch))]
                batch_results.sort(key=lambda x: x[0])

                for index, json_dict in batch_results:
                    for record in json_dict:

                        stream = record["stream"]
                        event_type = record["data"]["e"]
                        event_time = record["data"]["E"]

                        if market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
                            transaction_time = record["data"]["T"]
                            final_update_id_in_last_stream = record["data"]["pu"]
                        if market == Market.COIN_M_FUTURES:
                            ps_unknown_field_coin_m_diff_depth_only = record["data"]["ps"]

                        symbol = record["data"]["s"]
                        first_update = record["data"]["U"]
                        final_update = record["data"]["u"]
                        bids = record["data"]["b"]
                        asks = record["data"]["a"]
                        timestamp_of_receive = record["_E"]

                        if market == Market.USD_M_FUTURES:
                            for bid in bids:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        transaction_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        final_update_id_in_last_stream,
                                        0,
                                        str(bid[0]),
                                        str(bid[1])
                                    ]
                                )
                            for ask in asks:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        transaction_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        final_update_id_in_last_stream,
                                        1,
                                        str(ask[0]),
                                        str(ask[1]),
                                    ]
                                )
                        elif market == Market.COIN_M_FUTURES:
                            for bid in bids:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        transaction_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        final_update_id_in_last_stream,
                                        0,
                                        str(bid[0]),
                                        str(bid[1]),
                                        ps_unknown_field_coin_m_diff_depth_only
                                    ]
                                )
                            for ask in asks:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        transaction_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        final_update_id_in_last_stream,
                                        1,
                                        str(ask[0]),
                                        str(ask[1]),
                                        ps_unknown_field_coin_m_diff_depth_only
                                    ]
                                )
                        elif market == Market.SPOT:
                            for bid in bids:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        0,
                                        str(bid[0]),
                                        str(bid[1])
                                    ]
                                )
                            for ask in asks:
                                records.append(
                                    [
                                        timestamp_of_receive,
                                        stream,
                                        event_type,
                                        event_time,
                                        symbol,
                                        first_update,
                                        final_update,
                                        1,
                                        str(ask[0]),
                                        str(ask[1]),
                                    ]
                                )
                    bar()
                del batch_results

        if market == Market.SPOT:
            columns = [
                "TimestampOfReceive",
                "Stream",
                "EventType",
                "EventTime",
                "Symbol",
                "FirstUpdateId",
                "FinalUpdateId",
                "IsAsk",
                "Price",
                "Quantity"
            ]
        elif market == Market.USD_M_FUTURES:
            columns = [
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
                "Quantity"
            ]
        elif market == Market.COIN_M_FUTURES:
            columns = [
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

        return pd.DataFrame(data=records, columns=columns)

    def _trade_stream_type_download_handler(self, list_of_file_to_be_downloaded: list[str], market: Market) -> pd.DataFrame:
        import pandas as pd
        from alive_progress import alive_bar

        len_of_files_to_be_downloaded = len(list_of_file_to_be_downloaded)

        records = []
        with alive_bar(len_of_files_to_be_downloaded, force_tty=True, spinner='dots_waves', title='') as bar:
            for i in range(0, len_of_files_to_be_downloaded, self.amount_of_files_to_be_downloaded_at_once):
                batch = list_of_file_to_be_downloaded[i:i + self.amount_of_files_to_be_downloaded_at_once]
                result_queue = queue.Queue()
                threads_list = []

                for idx, file_name in enumerate(batch, start=i):
                    thread = threading.Thread(target=self.download_file_and_put_json_into_queue,
                                              args=(file_name, result_queue, idx))
                    threads_list.append(thread)
                    thread.start()

                for thread in threads_list:
                    thread.join()

                batch_results = [result_queue.get() for _ in range(len(batch))]
                batch_results.sort(key=lambda x: x[0])

                for index, json_dict in batch_results:
                    for record in json_dict:
                        stream = record["stream"]
                        event_type = record["data"]["e"]
                        event_time = record["data"]["E"]
                        transaction_time = record["data"]["T"]
                        symbol = record["data"]["s"]
                        trade_id = record["data"]["t"]
                        price = str(record["data"]["p"])
                        quantity = str(record["data"]["q"])
                        # seller_order_id = record["data"]["a"] if "a" in record["data"] else None # what the fuck was that?
                        # buyer_order_id = record["data"]["b"] if "b" in record["data"] else None # what the fuck was that?
                        is_buyer_market_maker = int(record["data"]["m"])
                        timestamp_of_receive = record["_E"]

                        if market == Market.SPOT:
                            large_m_unknown_parameter = record["data"]["M"]

                        if market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
                            large_x_unknown_parameter = record["data"]["X"]

                        if market == Market.SPOT:
                            records.append(
                                [
                                    timestamp_of_receive,
                                    stream,
                                    event_type,
                                    event_time,
                                    transaction_time,
                                    symbol,
                                    trade_id,
                                    price,
                                    quantity,
                                    is_buyer_market_maker,
                                    large_m_unknown_parameter
                                ]
                            )
                        elif market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
                            records.append(
                                [
                                    timestamp_of_receive,
                                    stream,
                                    event_type,
                                    event_time,
                                    transaction_time,
                                    symbol,
                                    trade_id,
                                    price,
                                    quantity,
                                    is_buyer_market_maker,
                                    large_x_unknown_parameter
                                ]
                            )

                    bar()

            columns = []

            if market == Market.SPOT:
                columns = [
                    "TimestampOfReceive",
                    "Stream",
                    "EventType",
                    "EventTime",
                    "TransactionTime",
                    "Symbol",
                    "TradeId",
                    "Price",
                    "Quantity",
                    "IsBuyerMarketMaker",
                    "MUnknownParameter"
                ]
            elif market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
                columns = [
                    "TimestampOfReceive",
                    "Stream",
                    "EventType",
                    "EventTime",
                    "TransactionTime",
                    "Symbol",
                    "TradeId",
                    "Price",
                    "Quantity",
                    "IsBuyerMarketMaker",
                    "XUnknownParameter"
                ]

            return pd.DataFrame(data=records, columns=columns)

    def _depth_snapshot_stream_type_download_handler(self, list_of_file_to_be_downloaded: list[str], market: Market) -> pd.DataFrame:
        import pandas as pd
        from alive_progress import alive_bar

        len_of_files_to_be_downloaded = len(list_of_file_to_be_downloaded)

        records = []
        with alive_bar(len_of_files_to_be_downloaded, force_tty=True, spinner='dots_waves', title='') as bar:
            for i in range(0, len_of_files_to_be_downloaded, self.amount_of_files_to_be_downloaded_at_once):
                batch = list_of_file_to_be_downloaded[i:i + self.amount_of_files_to_be_downloaded_at_once]
                result_queue = queue.Queue()
                threads_list = []

                for idx, file_name in enumerate(batch, start=i):
                    thread = threading.Thread(target=self.download_file_and_put_json_into_queue,
                                              args=(file_name, result_queue, idx))
                    threads_list.append(thread)
                    thread.start()

                for thread in threads_list:
                    thread.join()

                batch_results = [result_queue.get() for _ in range(len(batch))]
                batch_results.sort(key=lambda x: x[0])

                for index, json_dict in batch_results:
                    record = json_dict
                    last_update_id = record["lastUpdateId"]

                    if market in [Market.USD_M_FUTURES, Market.COIN_M_FUTURES]:
                        message_output_time = record["E"]
                        transaction_time = record["T"]
                    if market is Market.COIN_M_FUTURES:
                        symbol = record['symbol']
                        pair = record['pair']

                    snapshot_request_timestamp = record["_rq"]
                    snapshot_receive_timestamp = record["_rc"]

                    if market is Market.SPOT:
                        for side in ['bids', 'asks']:
                            for price_level in record[side]:
                                is_ask = 0 if side == 'bids' else 1
                                records.append(
                                    [
                                        snapshot_receive_timestamp,
                                        snapshot_request_timestamp,
                                        last_update_id,
                                        is_ask,
                                        str(price_level[0]),
                                        str(price_level[1])
                                    ]
                                )
                    if market is Market.USD_M_FUTURES:
                        for side in ['bids', 'asks']:
                            for price_level in record[side]:
                                is_ask = 0 if side == 'bids' else 1
                                records.append(
                                    [
                                        snapshot_receive_timestamp,
                                        snapshot_request_timestamp,
                                        message_output_time,
                                        transaction_time,
                                        last_update_id,
                                        is_ask,
                                        str(price_level[0]),
                                        str(price_level[1])
                                    ]
                                )
                    if market is Market.COIN_M_FUTURES:
                        for side in ['bids', 'asks']:
                            for price_level in record[side]:
                                is_ask = 0 if side == 'bids' else 1
                                records.append(
                                    [
                                        snapshot_receive_timestamp,
                                        snapshot_request_timestamp,
                                        message_output_time,
                                        transaction_time,
                                        last_update_id,
                                        symbol,
                                        pair,
                                        is_ask,
                                        str(price_level[0]),
                                        str(price_level[1])
                                    ]
                                )
                    bar()

        if market == Market.SPOT:
            columns = [
                "TimestampOfReceive",
                "TimestampOfRequest",
                "LastUpdateId",
                "IsAsk",
                "Price",
                "Quantity"
            ]
        elif market == Market.USD_M_FUTURES:
            columns = [
                "TimestampOfReceive",
                "TimestampOfRequest",
                "MessageOutputTime",
                "TransactionTime",
                "LastUpdateId",
                "IsAsk",
                "Price",
                "Quantity"
            ]
        elif market == Market.COIN_M_FUTURES:
            columns = [
                "TimestampOfReceive",
                "TimestampOfRequest",
                "MessageOutputTime",
                "TransactionTime",
                "LastUpdateId",
                "Symbol",
                "Pair",
                "IsAsk",
                "Price",
                "Quantity"
            ]

        return pd.DataFrame(data=records, columns=columns)

    def download_file_and_put_json_into_queue(self, file_name: str, result_queue: queue.Queue, index: int) -> None:
        while True:
            try:
                response = self.storage_client.read_file(file_name=file_name)
                json_dict = self._convert_cloud_storage_response_to_json(response)
                result_queue.put((index, json_dict))
                break

            except Exception as e:
                print(f"Error downloading {file_name}: {e}")
                print("Retry in 10 seconds...")
                time.sleep(10)

    @staticmethod
    def _convert_cloud_storage_response_to_json(storage_response: bytes) -> list[dict] | None:
        try:
            with zipfile.ZipFile(io.BytesIO(storage_response)) as z:
                for file_name in z.namelist():
                    if file_name.endswith('.json'):
                        with z.open(file_name) as json_file:
                            json_bytes = json_file.read()
                            json_content = orjson.loads(json_bytes)
                            return json_content

            print("file .json not found in archive")
        except zipfile.BadZipFile:
            print("bad zip file")
            return None
        except orjson.JSONDecodeError:
            print("error during json decode")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None


class IClientHandler(ABC):
    __slots__ = ()

    @abstractmethod
    def list_files_with_prefixes(self, prefixes: list[str]) -> list[str]:
        ...

    @abstractmethod
    def read_file(self, file_name) -> bytes:
        ...


class BackBlazeS3Client(IClientHandler):

    __slots__ = ['_bucket_name', 's3_client']

    def __init__(
            self,
            access_key_id: str,
            secret_access_key: str,
            endpoint_url: str,
            bucket_name: str
    ) -> None:
        import boto3

        self._bucket_name = bucket_name

        self.s3_client = boto3.client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url
        )

    def list_files_with_prefixes(self, prefixes: str | list[str]) -> list[str]:

        if isinstance(prefixes, str):
            prefixes = [prefixes]

        all_files = []
        for prefix in prefixes:
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=self._bucket_name, Prefix=prefix)

                files = []
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            files.append(obj['Key'])

                if files:
                    # print(f"Found {len(files)} files with '{prefix}' prefix in '{self._bucket_name}' bucket")
                    all_files.extend(files)
                else:
                    ...
                    # print(f"No files with '{prefix}' prefix in '{self._bucket_name}' bucket")

            except Exception as e:
                print(f"Error whilst listing '{prefix}': {e}")

        return all_files

    def read_file(self, file_name) -> bytes:
        response = self.s3_client.get_object(Bucket=self._bucket_name, Key=file_name)
        return response['Body'].read()


class AzureClient(IClientHandler):
    __slots__ = ()

    def __init__(
            self,
            blob_connection_string: str,
            container_name: str
    ) -> None:
        ...

    def list_files_with_prefixes(self, prefixes: str | list[str]) -> list[str]:
        ...

    def read_file(self, file_name) -> None:
        ...
