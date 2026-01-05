from __future__ import annotations

from binance_data_processor.core.logo import binance_archiver_logo
from binance_data_processor.scraper.data_downloader import DataDownloader
from binance_data_processor.scraper.scraper_config import DataScraperConfig
from binance_data_processor.data_quality.data_quality_report import DataQualityReport
from binance_data_processor.data_quality.data_quality_checker import get_df_quality_report
from binance_data_processor.enums.asset_parameters import AssetParameters
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.utils.file_utils import get_list_of_existing_root_csvs_asset_parameters
from binance_data_processor.utils.file_utils import get_base_of_root_csv_filename
from binance_data_processor.utils.file_utils import to_csv_w_data_quality_reports


__all__ = [
    'download_csv_data'
]


def download_csv_data(cfg: DataScraperConfig) -> None:
    data_scraper = DataScraper()
    data_scraper.run(cfg=cfg)


class DataScraper:

    __slots__ = [
        'data_downloader'
    ]

    def __init__(self) -> None:
        self.data_downloader = DataDownloader()

    def run(self, cfg: DataScraperConfig) -> None:
        if cfg.verbose: print(f'\033[35m{binance_archiver_logo}')
        self._main_loop(cfg)

    def _main_loop(self, cfg: DataScraperConfig) -> None:
        print(f'\033[36m')

        asset_parameters_list = self._get_final_asset_parameters_list_to_be_downloaded(cfg)

        print(f'ought to download {len(asset_parameters_list)} file(s):\n')
        if cfg.verbose: DataScraper.print_asset_parameters_list(asset_parameters_list)

        data_quality_reports = []

        for ap in asset_parameters_list:
            print(f'Downloading: {ap}')
            df = self.data_downloader.download_as_df(ap)
            df_quality_report = get_df_quality_report(df, ap)
            print(f'data report status: {df_quality_report.get_status().value}\n')
            data_quality_reports.append(df_quality_report)

            to_csv_w_data_quality_reports(
                df=df,
                data_quality_reports=df_quality_report,
                dump_catalog=cfg.dump_path,
                filename=get_base_of_root_csv_filename(ap)
            )

            del df_quality_report, df

        if cfg.verbose:
            DataScraper.show_data_quality_report_list_summary(data_quality_reports)

    @staticmethod
    def _get_final_asset_parameters_list_to_be_downloaded(cfg: DataScraperConfig) -> list[AssetParameters]:
        all_assets = [
            AssetParameters(
                market=m,
                stream_type=s,
                pairs=[(f'{p[:-1]}_perp'.lower() if m is Market.COIN_M_FUTURES else p.lower())],
                date=d
            )
            for p in cfg.pairs
            for m in cfg.markets
            for s in cfg.stream_types
            for d in cfg.dates_to_be_downloaded
        ]
        if cfg.skip_existing:
            existing = get_list_of_existing_root_csvs_asset_parameters(cfg.dump_path)
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
            print(f'{i}. {report.asset_parameters}: {report.get_status()}')
            i += 1

        while True:
            try:
                user_input = input('\n(n/exit): ').strip().lower()
                if user_input == 'exit':
                    break
                print(data_quality_report_list[int(user_input) - 1])
            except Exception as e:
                print(e)
