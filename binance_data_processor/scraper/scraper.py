from __future__ import annotations

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.scraper.data_downloader import DataDownloader
from binance_data_processor.scraper.scraper_config import DataScraperConfig
from binance_data_processor.data_quality.data_quality_report import DataQualityReport
from binance_data_processor.data_quality.data_quality_checker import get_dataframe_quality_report
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.utils.file_utils import get_list_of_existing_root_csvs_asset_parameters
from binance_data_processor.utils.file_utils import get_base_of_root_csv_filename
from binance_data_processor.utils.file_utils import save_df_with_data_quality_reports


__all__ = [
    'download_csv_data'
]


def download_csv_data(config: DataScraperConfig) -> None:
    data_scraper = DataScraper()
    data_scraper.run(config=config)


class DataScraper:

    __slots__ = [
        'data_downloader'
    ]

    def __init__(self) -> None:
        self.data_downloader = DataDownloader()

    def run(self, config: DataScraperConfig) -> None:
        if config.verbose: print(f'\033[35m{binance_archiver_logo}')

        self._main_download_loop(config=config)

    def _main_download_loop(self, config: DataScraperConfig) -> None:
        print(f'\033[36m')

        asset_parameters_list = self._get_final_asset_parameters_list_to_be_downloaded(config)

        print(f'ought to download {len(asset_parameters_list)} file(s):\n')
        if config.verbose: DataScraper.print_asset_parameters_list(asset_parameters_list)

        data_quality_report_list = []

        for asset_parameters in asset_parameters_list:
            print(f'Downloading: {asset_parameters}')
            dataframe = self.data_downloader.download_asset_parameters_as_data_frame(asset_parameters)
            dataframe_quality_report = get_dataframe_quality_report(
                dataframe=dataframe,
                asset_parameters=asset_parameters
            )
            print(f'data report status: {dataframe_quality_report.get_data_report_status().value}')
            print()
            data_quality_report_list.append(dataframe_quality_report)

            target_file_name = get_base_of_root_csv_filename(asset_parameters=asset_parameters)

            save_df_with_data_quality_reports(
                dataframe=dataframe,
                dataframe_quality_reports=dataframe_quality_report,
                dump_catalog=config.dump_path,
                filename=target_file_name
            )

            del dataframe_quality_report, dataframe

        if config.verbose:
            DataScraper.show_data_quality_report_list_summary(data_quality_report_list)

    @staticmethod
    def _get_final_asset_parameters_list_to_be_downloaded(config: DataScraperConfig) -> list[AssetParameters]:
        all_assets = [
            AssetParameters(
                market=m,
                stream_type=s,
                pairs=[(f'{p[:-1]}_perp'.lower() if m is Market.COIN_M_FUTURES else p.lower())],
                date=d
            )
            for p in config.pairs
            for m in config.markets
            for s in config.stream_types
            for d in config.dates_to_be_downloaded
        ]
        if config.skip_existing:
            existing = get_list_of_existing_root_csvs_asset_parameters(config.dump_path)
            return [a for a in all_assets if a not in existing]
        return all_assets

    @staticmethod
    def print_asset_parameters_list(asset_parameters_list: list[AssetParameters]) -> None:
        rows = []
        for ap in asset_parameters_list:
            rows.append((
                ap.pairs[0].upper(),
                ap.market.name,
                ap.stream_type.name,
                ap.date or ''
            ))

        col_widths = [
            max(len(str(item)) for item in column)
            for column in zip(*rows)
        ]

        for pair, market, stream, date in rows:
            print(
                f"{pair:<{col_widths[0]}} "
                f"{market:<{col_widths[1]}} "
                f"{stream:<{col_widths[2]}} "
                f"{date}"
            )

    @staticmethod
    def show_data_quality_report_list_summary(data_quality_report_list: list[DataQualityReport]) -> None:
        if len(data_quality_report_list) == 0:
            return

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
