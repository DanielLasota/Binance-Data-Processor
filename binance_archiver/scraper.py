from __future__ import annotations

import io
import os
import queue
import threading
import zipfile
from datetime import timedelta, datetime
import boto3
import orjson
from alive_progress import alive_bar
import pandas as pd
from abc import ABC, abstractmethod
from typing import List

from binance_archiver.data_quality_checker import get_dataframe_quality_report, DataQualityChecker
from binance_archiver.data_quality_report import DataQualityReport
from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.enum_.epoch_time_unit import EpochTimeUnit
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.logo import binance_archiver_logo
from binance_archiver.enum_.storage_connection_parameters import StorageConnectionParameters


__all__ = [
    'download_csv_data'
]


def download_csv_data(
        date_range: list[str],
        storage_connection_parameters: StorageConnectionParameters,
        dump_path: str | None = None,
        pairs: list[str] | None = None,
        markets: list[str] | None = None,
        stream_types: list[str] | None = None,
        skip_existing: bool = False
        ) -> None:

    data_scraper = DataScraper(storage_connection_parameters)

    data_scraper.run(
        markets=markets,
        stream_types=stream_types,
        pairs=pairs,
        date_range=date_range,
        dump_path=dump_path,
        skip_existing=skip_existing
    )


class DataScraper:

    __slots__ = [
        'storage_client'
    ]

    def __init__(
            self,
            storage_connection_parameters
    ) -> None:

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

    def run(
            self,
            markets: list[str],
            stream_types: list[str],
            pairs: list[str],
            date_range: list[str],
            dump_path: str | None,
            skip_existing: bool = True
    ) -> None:

        print(f'\033[35m{binance_archiver_logo}')

        dump_path = self._prepare_dump_path_catalog(dump_path=dump_path)

        dates_to_be_downloaded = self._generate_dates_string_list_from_range(date_range)

        asset_parameters_to_be_downloaded = self._get_asset_parameters_to_be_downloaded(
            markets=markets,
            stream_types=stream_types,
            pairs=pairs,
            dates_to_be_downloaded=dates_to_be_downloaded,
            dump_path=dump_path,
            skip_existing=skip_existing
        )
        amount_of_files_to_be_made = len(asset_parameters_to_be_downloaded) * len(dates_to_be_downloaded)

        print(
            f'\033[36m'
            f'ought to download {amount_of_files_to_be_made} file(s)\n'
        )

        self._main_download_loop(
            asset_parameters_list=asset_parameters_to_be_downloaded,
            dates=dates_to_be_downloaded,
            dump_path=dump_path
        )

        print(f'\nFinished: {dump_path}')

    def _get_asset_parameters_to_be_downloaded(self, markets: list[str], stream_types: list[str], pairs: list[str], dates_to_be_downloaded: list[str], dump_path: str, skip_existing: bool) -> list[AssetParameters]:
        selected_asset_parameters = self._generate_asset_parameters_data_classes_list(
            markets=markets,
            stream_types=stream_types,
            pairs=pairs,
            dates=dates_to_be_downloaded
        )

        if skip_existing is True:
            asset_parameters_of_existing_files = self._get_existing_files_asset_parameters_list(csv_nest=dump_path)
            final_asset_parameters_to_be_downloaded = [
                asset for asset
                in selected_asset_parameters
                if asset not in asset_parameters_of_existing_files
            ]
            selected_and_existing_to_be_skipped = [
                asset for asset
                in selected_asset_parameters
                if asset in asset_parameters_of_existing_files
            ]

            print(f'skipping existing assets:')
            for asset in selected_and_existing_to_be_skipped:
                print(asset)
        else:
            final_asset_parameters_to_be_downloaded = selected_asset_parameters

        return final_asset_parameters_to_be_downloaded

    @staticmethod
    def _prepare_dump_path_catalog(dump_path) -> str:
        if dump_path is None:
            dump_path = os.path.join(
                os.path.expanduser("~"),
                "Documents",
                "binance_archival_data"
            ).replace('\\', '/')
        if not os.path.exists(dump_path):
            os.makedirs(dump_path)
        os.startfile(dump_path)

        return dump_path

    @staticmethod
    def _get_existing_files_asset_parameters_list(csv_nest: str) -> list[AssetParameters]:
        local_files = DataQualityChecker.list_files_in_local_directory(csv_nest)
        local_csv_file_paths = [file for file in local_files if file.lower().endswith('.csv')]
        print(f"Found {len(local_csv_file_paths)} CSV files out of {len(local_files)} total files")

        found_asset_parameter_list = []

        for csv in local_csv_file_paths:
            asset_parameters = DataQualityChecker.decode_asset_parameters_from_csv_name(csv)
            found_asset_parameter_list.append(asset_parameters)

        return found_asset_parameter_list

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
    def _generate_asset_parameters_data_classes_list(markets: list[str], stream_types: list[str], pairs: list[str], dates: list[str]) -> list[AssetParameters]:
        markets = [Market(market.lower()) for market in markets]
        stream_types = [StreamType[stream_type.upper()] for stream_type in stream_types]
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

    def _main_download_loop(self, asset_parameters_list: list[AssetParameters], dates: list[str], dump_path: str) -> None:
        for date in dates:
            for asset_parameters in asset_parameters_list:
                print(f'Downloading: {asset_parameters}')
                dataframe = self._get_rough_dataframe_from_cloud_storage_files_cut_to_specified_date(asset_parameters, date)

                dataframe_quality_report = get_dataframe_quality_report(
                    dataframe=dataframe,
                    asset_parameters=asset_parameters,
                )
                print(f'data report status: {dataframe_quality_report.get_data_report_status().value}')

                # minimal_dataframe = self._get_minimal_dataframe(df=rough_dataframe_for_quality_check)

                target_file_name = DataScraper._get_base_of_filename(asset_parameters=asset_parameters, date=date)

                self._save_df_to_csv_with_data_quality_report(
                    dataframe=dataframe,
                    dataframe_quality_report=dataframe_quality_report,
                    dump_path=dump_path,
                    target_file_name=target_file_name
                )

    def _get_rough_dataframe_from_cloud_storage_files_cut_to_specified_date(self, asset_parameters: AssetParameters, date: str) -> pd.DataFrame:
        list_of_files_with_specified_prefixes_that_should_be_downloaded = (
            self._get_prefix_of_files_that_should_be_downloaded_for_specified_single_date(
                asset_parameters=asset_parameters,
                date=date
            )
        )

        list_of_files_to_be_downloaded = self.storage_client.list_files_with_prefixes(list_of_files_with_specified_prefixes_that_should_be_downloaded)

        stream_type_download_handler = self._get_stream_type_download_handler(asset_parameters.stream_type)
        dataframe = stream_type_download_handler(list_of_files_to_be_downloaded, asset_parameters.market)

        epoch_time_unit = (
            EpochTimeUnit.MICROSECONDS
            if asset_parameters.market is Market.SPOT
            else EpochTimeUnit.MILLISECONDS
        )

        dataframe = self._cut_dataframe_to_the_range_of_single_day(
            dataframe=dataframe,
            target_day=date,
            epoch_time_unit=epoch_time_unit
        )

        return dataframe

    @staticmethod
    def _save_df_to_csv_with_data_quality_report(dataframe: pd.DataFrame, dump_path: str, target_file_name: str,dataframe_quality_report: DataQualityReport) -> None:
        with open(f'{dump_path}/{target_file_name}.csv', 'w', newline='') as f:
            f.write(str(dataframe_quality_report))
            f.write('\n')
            dataframe.to_csv(f, index=False, lineterminator='\n')

    @staticmethod
    def _cut_dataframe_to_the_range_of_single_day(dataframe: pd.DataFrame, target_day: str, epoch_time_unit: EpochTimeUnit = EpochTimeUnit.MILLISECONDS) -> pd.DataFrame:
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
    def _get_minimal_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(
            columns=[
                'Stream',
                'EventType',
                'Symbol',
                'PSUnknownField',
                'XUnknownParameter',
                'MUnknownParameter'
            ],
            errors='ignore'
        )

    @staticmethod
    def _get_base_of_filename(asset_parameters: AssetParameters, date: str) -> str:
        return f"binance_{asset_parameters.stream_type.name.lower()}_{asset_parameters.market.name.lower()}_{asset_parameters.pairs[0].lower()}_{date}"

    @staticmethod
    def _get_prefix_of_files_that_should_be_downloaded_for_specified_single_date(asset_parameters: AssetParameters, date: str) -> list[str]:
        one_date_after = (datetime.strptime(date, '%d-%m-%Y') + timedelta(days=1)).strftime('%d-%m-%Y')

        target_day_date_prefix = DataScraper._get_base_of_filename(asset_parameters=asset_parameters, date=date)
        day_date_one_after_prefix = DataScraper._get_base_of_filename(asset_parameters=asset_parameters, date=one_date_after) + 'T00-0'

        prefixes_list = [target_day_date_prefix, day_date_one_after_prefix]

        return prefixes_list

    def _get_stream_type_download_handler(self, stream_type: StreamType) -> callable:
        handler_lookup = {
            StreamType.DIFFERENCE_DEPTH_STREAM: self._difference_depth_stream_type_download_handler,
            StreamType.TRADE_STREAM: self._trade_stream_type_download_handler,
            StreamType.DEPTH_SNAPSHOT: self._difference_depth_snapshot_stream_type_download_handler
        }
        return handler_lookup[stream_type]

    def _difference_depth_stream_type_download_handler(self, list_of_file_to_be_downloaded: list[str], market: Market) -> pd.DataFrame:

        len_of_files_to_be_downloaded = len(list_of_file_to_be_downloaded)
        amount_of_files_to_be_downloaded_at_once = 20

        records = []
        with alive_bar(len_of_files_to_be_downloaded, force_tty=True, spinner='dots_waves', title='') as bar:
            for i in range(0, len_of_files_to_be_downloaded, amount_of_files_to_be_downloaded_at_once):
                batch = list_of_file_to_be_downloaded[i:i + amount_of_files_to_be_downloaded_at_once]
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

        len_of_files_to_be_downloaded = len(list_of_file_to_be_downloaded)
        amount_of_files_to_be_downloaded_at_once = 20

        records = []
        with alive_bar(len_of_files_to_be_downloaded, force_tty=True, spinner='dots_waves', title='') as bar:
            for i in range(0, len_of_files_to_be_downloaded, amount_of_files_to_be_downloaded_at_once):
                batch = list_of_file_to_be_downloaded[i:i + amount_of_files_to_be_downloaded_at_once]
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

    def _difference_depth_snapshot_stream_type_download_handler(self, files_list_to_download: list[str]) -> pd.DataFrame:
        ...

    def download_file_and_put_json_into_queue(self, file_name: str, result_queue: queue.Queue, index: int) -> None:
        try:
            response = self.storage_client.read_file(file_name=file_name)
            json_dict = self._convert_cloud_storage_response_to_json(response)
            result_queue.put((index, json_dict))
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")
            result_queue.put((index, None))

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
    def list_files_with_prefixes(self, prefixes: List[str]) -> List[str]:
        pass

    @abstractmethod
    def read_file(self, file_name) -> bytes:
        pass


class BackBlazeS3Client(IClientHandler):

    __slots__ = ['_bucket_name', 's3_client']

    def __init__(
            self,
            access_key_id: str,
            secret_access_key: str,
            endpoint_url: str,
            bucket_name: str
    ) -> None:

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
