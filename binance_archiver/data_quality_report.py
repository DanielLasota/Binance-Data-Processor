from dataclasses import dataclass

import pandas as pd

from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.enum_.data_quality_report_status_enum import DataQualityReportStatus


@dataclass
class DataQualityReport:

    __slots__ = [
        'tests_results_register',
        'asset_parameters',
        'informational_data'
    ]

    def __init__(
            self,
            asset_parameters: AssetParameters
    ):
        self.tests_results_register: {str, {str, bool | str}} = {}
        self.asset_parameters = asset_parameters
        self.informational_data = []

    def is_data_quality_report_positive(self) -> bool:
        return all(
            bool(result) is True
            for tests in self.tests_results_register.values()
            for result in tests.values()
        )

    def get_data_report_status(self) -> DataQualityReportStatus:
        return (
            DataQualityReportStatus.POSITIVE
            if self.is_data_quality_report_positive() == True
            else DataQualityReportStatus.NEGATIVE
        )

    def __str__(self) -> str:
        from datetime import datetime

        data_quality_status = 'POSITIVE' if self.is_data_quality_report_positive() == True else 'NEGATIVE'

        lines = [
            f"#################################################################",
            f"# Data Quality Report for {self.asset_parameters.stream_type} {self.asset_parameters.market.value} {self.asset_parameters.pairs[0]}",
            f"# Generated on: {datetime.utcnow().strftime('%d-%m-%YT%H:%M:%S.%fZ')[:-3]}"
        ]

        if len(self.informational_data) > 0:
            for entry in self.informational_data:
                lines.append(f'# {entry}')

        raw_tests_results_list = []
        for column, tests in self.tests_results_register.items():
            for test_name, result in tests.items():
                status = "PASS" if result == True else f"FAIL - {result}" if isinstance(result, str) else "FAIL"
                raw_tests_results_list.append((column, test_name, status))

        if raw_tests_results_list:
            max_column_length = max(len(row[0]) for row in raw_tests_results_list) + 5
            max_test_name_length = max(len(row[1]) for row in raw_tests_results_list) + 5
            max_status_length = max(len(row[2]) for row in raw_tests_results_list) + 5

            df = pd.DataFrame(raw_tests_results_list, columns=['Column', 'TestName', 'Status'])
            df_str = df.to_string(index=False)
            df_lines = df_str.split('\n')

            lines.append(f'# Data Quality Report Status: {data_quality_status}')
            lines.append(f'# ------------------------------------')
            for line in df_lines:
                if line.strip():
                    parts = line.split()
                    if len(parts) == 3:
                        column, test_name, status = parts
                        aligned_line = f"# {column.ljust(max_column_length)} {test_name.ljust(max_test_name_length)} {status.ljust(max_status_length)}"
                        lines.append(aligned_line)

        lines.append("# End of Quality Report")
        lines.append(f"#################################################################")
        return "\n".join(lines)

    def add_test_result(self, column: str, test_name: str, result: bool) -> None:
        if column not in self.tests_results_register:
            self.tests_results_register[column] = {}
        self.tests_results_register[column][test_name] = result

    def add_informational_data_to_report(self, column: str, information: str) -> None:
        self.informational_data.append(f'{column}: {information}')

    def is_valid(self) -> bool:
        return all(
            isinstance(result, bool) and result
            for tests in self.tests_results_register.values()
            for result in tests.values()
        )