from __future__ import annotations

from binance_data_processor.core.logo import logo1
from binance_data_processor.enums.quality_report_status_enum import QualityStatus
from binance_data_processor.scraper.data_downloader import DataDownloader
from binance_data_processor.scraper.scraper_config import ScraperCfg
from binance_data_processor.data_quality.quality_report import QualityReport
from binance_data_processor.data_quality.quality_checker import get_df_quality_report
from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.market_enum import Market
from binance_data_processor.utils.file_utils import get_list_of_existing_root_csvs_asset_parameters
from binance_data_processor.utils.file_utils import get_base_of_root_csv_filename
from binance_data_processor.utils.file_utils import to_csv_w_quality_reports


__all__ = [
    'download_csv_data'
]


def download_csv_data(cfg: ScraperCfg) -> None:
    data_scraper = DataScraper()
    data_scraper.run(cfg=cfg)


class DataScraper:

    __slots__ = [
        'data_downloader'
    ]

    def __init__(self) -> None:
        self.data_downloader = DataDownloader()

    def run(self, cfg: ScraperCfg) -> None:
        print(f'\033[35m{logo1}\033[0m')
        self._main_loop(cfg)

    def _main_loop(self, cfg: ScraperCfg) -> None:
        print(f'\033[36m')
        ap_list = self._get_final_ap_list_to_be_downloaded(cfg)
        print(f'ought to download {len(ap_list)} file(s):\n')

        if cfg.verbose:
            DataScraper.print_ap_list(ap_list)

        reports = []

        for ap in ap_list:
            print(f'Downloading: {ap}')
            df = self.data_downloader.download_as_df(ap)
            report = get_df_quality_report(df, ap)
            print(f'data report status: {report.get_status().value}')
            if report.get_status() is QualityStatus.NEGATIVE:
                print(f'failed tests: \n{report.get_fails()}\n')

            reports.append(report)

            to_csv_w_quality_reports(
                df=df,
                quality_reports=report,
                dump_dir=cfg.dump_path,
                fname=get_base_of_root_csv_filename(ap)
            )

            del report, df

        if cfg.verbose:
            DataScraper.show_quality_report_list_summary(reports)

    @staticmethod
    def _get_final_ap_list_to_be_downloaded(cfg: ScraperCfg) -> list[AssetParams]:
        ap_list = [
            AssetParams(
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
            return [a for a in ap_list if a not in existing]
        return ap_list

    @staticmethod
    def print_ap_list(ap_list: list[AssetParams]) -> None:
        rows = []
        for ap in ap_list:
            rows.append((ap.pairs[0].upper(), ap.market.name, ap.stream_type.name, ap.date or ''))

        col_widths = [max(len(str(item)) for item in col) for col in zip(*rows)]

        for pair, market, stream, date in rows:
            print(f"{pair:<{col_widths[0]}} {market:<{col_widths[1]}} {stream:<{col_widths[2]}} {date}")

    @staticmethod
    def show_quality_report_list_summary(quality_report_list: list[QualityReport]) -> None:
        if len(quality_report_list) == 0:
            return

        i = 1
        for report in quality_report_list:
            print(f'{i}. {report.ap}: {report.get_status()}')
            i += 1

        while True:
            try:
                user_input = input('\n(n/exit): ').strip().lower()
                if user_input == 'exit':
                    break
                print(quality_report_list[int(user_input) - 1])
            except Exception as e:
                print(e)
