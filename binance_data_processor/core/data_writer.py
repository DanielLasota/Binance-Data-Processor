import time

from binance_data_processor import DataSinkCfg
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.enums.save_target_enum import SaveTarget
from binance_data_processor.utils.sys import debug_where_am_i


class DataWriter:
    __slots__ = (
        'cfg',
        'logger',
        '_client',
    )

    def __init__(
            self,
            cfg: DataSinkCfg,
    ):
        self.cfg = cfg
        self.logger = setup_logger(dump=True)
        self._client = None

        if self.cfg.save_target is SaveTarget.BACKBLAZE:
            from binance_data_processor.cloud_storage_clients.s3_client import S3Client
            self._client = S3Client(self.cfg.blob_conn_params)

    def save_data(self, data: str, dir_: str, f_name: str) -> None:
        match self.cfg.save_target:
            case SaveTarget.JSON:
                self.write_to_json(data, dir_, f_name)
            case SaveTarget.ZIP:
                self.write_to_zip(data, dir_, f_name)
            case SaveTarget.BACKBLAZE:
                self.send_zipped_json(data, dir_, f_name)
            case _:
                raise ValueError(f"Nieobsługiwany cel zapisu: {self.cfg.save_target!r}")

    def write_to_json(self, data: str, dir_: str, f_name: str) -> None:
        file_path = f"{dir_}/{f_name}.json"

        try:
            with open(file_path, "x", encoding="utf-8") as file:
                file.write(data)
        except OSError:
            self.logger.exception("Nie udało się zapisać %s", file_path)
            raise

    def write_to_zip(self, data: str, dir_: str, f_name: str) -> None:
        import zipfile

        file_path = f"{dir_}/{f_name}.zip"

        try:
            with zipfile.ZipFile(file_path, mode="x", compression=zipfile.ZIP_DEFLATED, compresslevel=9,) as archive:
                archive.writestr(f"{f_name}.json", data)
        except OSError:
            self.logger.exception("Nie udało się zapisać %s", file_path)
            raise

    def send_zipped_json(self, data: str, dir_: str, f_name: str) -> None:
        max_retries = 5
        retry_delay_seconds = 5

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f'sending zipped json: {f_name}')
                self._client.upload_zipped_jsoned_string(data, f_name)
                break
            except Exception as e:
                self.logger.info(f'Attempt {attempt}/{max_retries}: error whilst sending to blob {f_name}, error: {e}')
                if attempt < max_retries:
                    time.sleep(retry_delay_seconds)
                else:
                    self.logger.info(f'Max retries reached for {f_name}. Saving locally.')
                    self.write_to_zip(data, dir_, f_name)

    def stop(self):
        if self._client:
            self._client.stop()
