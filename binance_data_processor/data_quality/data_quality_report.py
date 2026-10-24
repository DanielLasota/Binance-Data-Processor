from dataclasses import dataclass

from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.data_quality_report_status_enum import DataQualityReportStatus


@dataclass
class DataQualityReport:

    __slots__ = [
        'tests_results_register',
        'asset_parameters',
        'informational_data',
        'file_name',
        'df_shape'
    ]

    def __init__(
            self,
            asset_parameters: list[AssetParams] | AssetParams,
            df_shape: list[int]
    ):
        self.tests_results_register: {str, {str, bool | str}} = {}
        self.asset_parameters = asset_parameters
        self.informational_data = []
        self.file_name = ...
        self.df_shape = df_shape

    def is_positive(self) -> bool:
        #todo: remove is_positive, keep get_status
        return all(
            bool(result) is True
            for tests in self.tests_results_register.values()
            for result in tests.values()
        )

    def get_status(self) -> DataQualityReportStatus:
        return DataQualityReportStatus.POSITIVE if self.is_positive() else DataQualityReportStatus.NEGATIVE

    def __str__(self) -> str:
        from datetime import datetime
        import pandas as pd

        if isinstance(self.asset_parameters, list):
            asset_parameters_str = "\n# ".join(str(f'{param}') for param in self.asset_parameters)
        else:
            asset_parameters_str = str(self.asset_parameters)

        lines = [
            f"#################################################################",
            f"# Unfazed Binance Data Processor. All Copyrights 2025 Daniel Lasota",
            f"# Data Quality Report for: \n# {asset_parameters_str}",
            f"# Generated on: {datetime.utcnow().strftime('%d-%m-%YT%H:%M:%S.%fZ')[:-4]}Z",
            f"# DataFrame shape: {self.df_shape[0]} x {self.df_shape[1]}"
        ]

        if len(self.informational_data) > 0:
            for entry in self.informational_data:
                lines.append(f'# {entry}')

        raw_tests_results_list = []
        for column, tests in self.tests_results_register.items():
            for test_name, result in tests.items():
                status = (
                    DataQualityReportStatus.POSITIVE.name if result == True
                    else DataQualityReportStatus.NEGATIVE.name
                )
                raw_tests_results_list.append((column, test_name, status))

        if raw_tests_results_list:
            max_column_length = max(len(row[0]) for row in raw_tests_results_list) + 5
            max_test_name_length = max(len(row[1]) for row in raw_tests_results_list) + 5
            max_status_length = max(len(row[2]) for row in raw_tests_results_list) + 5

            df = pd.DataFrame(raw_tests_results_list, columns=['Column', 'TestName', 'Status'])
            df_str = df.to_string(index=False)
            df_lines = df_str.split('\n')

            lines.append(f'# Data Quality Report Status: {self.get_status().value}')
            lines.append(f'# ------------------------------------')
            for line in df_lines:
                if line.strip():
                    parts = line.split()
                    if len(parts) == 3:
                        column, test_name, status = parts
                        aligned_line = f"# {column.ljust(max_column_length)} {test_name.ljust(max_test_name_length)} {status.ljust(max_status_length)}"
                        lines.append(aligned_line)

        hashtag_line = '#' * len(lines[-1])
        lines[0] = hashtag_line
        lines.append("# End of Quality Report")
        lines.append(hashtag_line)

        return "\n".join(lines)

    def add(self, col: str, test_name: str, result: bool) -> None:
        if col not in self.tests_results_register:
            self.tests_results_register[col] = {}
        self.tests_results_register[col][test_name] = result

    def add_info(self, column: str, information: str) -> None:
        self.informational_data.append(f'{column}: {information}')

    def get_fails(self) -> str:
        fails: list[str] = []

        for col, tests in self.tests_results_register.items():
            for test_name, result in tests.items():
                if not result:
                    fails.append(f" {col} {test_name} {result}")

        return ''.join(fails)
