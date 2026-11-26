from dataclasses import dataclass

from binance_data_processor.enums.asset_params import AssetParams
from binance_data_processor.enums.quality_report_status_enum import QualityStatus


@dataclass
class QualityReport:

    __slots__ = [
        'tests_results_register',
        'ap',
        'informational_data',
        'file_name',
        'df_shape'
    ]

    def __init__(
            self,
            ap: list[AssetParams] | AssetParams,
            df_shape: list[int]
    ):
        self.tests_results_register: {str, {str, bool | str}} = {}
        self.ap = ap
        self.informational_data = []
        self.file_name = ...
        self.df_shape = df_shape

    def is_positive(self) -> bool:
        #todo: remove is_positive, keep get_status
        return all(bool(result) is True for tests in self.tests_results_register.values() for result in tests.values())

    def get_status(self) -> QualityStatus:
        return QualityStatus.POSITIVE if self.is_positive() else QualityStatus.NEGATIVE

    def __str__(self) -> str:
        from datetime import datetime
        import pandas as pd

        if isinstance(self.ap, list):
            ap_str = "\n# ".join(str(f'{param}') for param in self.ap)
        else:
            ap_str = str(self.ap)

        res = [
            f"#################################################################",
            f"# Unfazed Binance Data Processor. All Copyrights 2025 Daniel Lasota",
            f"# Data Quality Report for: \n# {ap_str}",
            f"# Generated on: {datetime.utcnow().strftime('%d-%m-%YT%H:%M:%S.%fZ')[:-4]}Z",
            f"# DataFrame shape: {self.df_shape[0]} x {self.df_shape[1]}"
        ]

        if len(self.informational_data) > 0:
            for entry in self.informational_data:
                res.append(f'# {entry}')

        raw_tests_results_list = []
        for col, tests in self.tests_results_register.items():
            for test_name, result in tests.items():
                status = (QualityStatus.POSITIVE.name if result == True else QualityStatus.NEGATIVE.name)
                raw_tests_results_list.append((col, test_name, status))

        if raw_tests_results_list:
            max_col_len = max(len(row[0]) for row in raw_tests_results_list) + 5
            max_test_name_len = max(len(row[1]) for row in raw_tests_results_list) + 5
            max_status_len = max(len(row[2]) for row in raw_tests_results_list) + 5

            df = pd.DataFrame(raw_tests_results_list, columns=['Column', 'TestName', 'Status'])
            df_str = df.to_string(index=False)
            df_lines = df_str.split('\n')

            res.append(f'# Data Quality Report Status: {self.get_status().value}')
            res.append(f'# ------------------------------------')
            for line in df_lines:
                if line.strip():
                    parts = line.split()
                    if len(parts) == 3:
                        col, test_name, status = parts
                        aligned_line = f"# {col.ljust(max_col_len)} {test_name.ljust(max_test_name_len)} {status.ljust(max_status_len)}"
                        res.append(aligned_line)

        hashtag_line = '#' * len(res[-1])
        res[0] = hashtag_line
        res.append("# End of Quality Report")
        res.append(hashtag_line)

        return "\n".join(res)

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
                    fails.append(f"     {col} {test_name} {result}")

        return '\n'.join(fails)
