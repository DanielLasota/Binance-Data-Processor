import io
import zipfile
import requests
import base64
import hashlib
import os
from typing import Tuple, Dict, Any, Optional
import time

from binance_data_processor.enums.storage_connection_parameters import StorageConnectionParameters


class B2Client:
    REFRESH_INTERVAL = 4 * 3600

    __slots__ = [
        'storage_connection_parameters',
        '_bucket_id',
        '_auth_token',
        '_api_url',
        '_account_id',
        '_session',
        '_last_refresh_time'
    ]

    def __init__(
            self,
            storage_connection_parameters: StorageConnectionParameters
    ) -> None:
        self.storage_connection_parameters = storage_connection_parameters
        self._bucket_id: str | None = None
        self._auth_token: str | None = None
        self._api_url: str | None = None
        self._account_id: str | None = None
        self._session = requests.Session()
        self._last_refresh_time = time.time()
        self._authenticate()

    def _refresh_session_if_needed(self) -> None:
        current_time = time.time()
        if current_time - self._last_refresh_time > self.REFRESH_INTERVAL:
            self._session.close()
            self._session = requests.Session()
            self._last_refresh_time = current_time
            print("Session refreshed")

    def _authenticate(self) -> None:
        self._refresh_session_if_needed()
        auth_string: str = f'{self.storage_connection_parameters.backblaze_access_key_id}:{self.storage_connection_parameters.backblaze_secret_access_key}'
        encoded_auth: str = base64.b64encode(auth_string.encode()).decode('utf-8')
        auth_headers: Dict[str, str] = {'Authorization': f'Basic {encoded_auth}'}
        auth_url: str = 'https://api.backblazeb2.com/b2api/v2/b2_authorize_account'

        response: requests.Response = self._session.get(auth_url, headers=auth_headers)
        try:
            response.raise_for_status()
            data: Dict[str, Any] = response.json()
            self._auth_token = data['authorizationToken']
            self._api_url = data['apiUrl']
            self._account_id = data.get('accountId')

            allowed_bucket_id = data.get("allowed", {}).get("bucketId")
            if allowed_bucket_id:
                self._bucket_id = allowed_bucket_id
        finally:
            response.close()

    def _get_bucket_id_from_name(self, bucket_name: str) -> str:
        self._refresh_session_if_needed()
        if self._account_id is None:
            raise Exception("Account ID nie został ustawiony. Upewnij się, że autoryzacja przebiegła poprawnie.")
        list_buckets_url: str = f'{self._api_url}/b2api/v2/b2_list_buckets'
        headers: Dict[str, str] = {"Authorization": self._auth_token}
        data = {"accountId": self._account_id}

        response: requests.Response = self._session.post(list_buckets_url, json=data, headers=headers)
        try:
            response.raise_for_status()
            buckets = response.json()["buckets"]
            for bucket in buckets:
                if bucket["bucketName"] == bucket_name:
                    return bucket["bucketId"]
            raise Exception(f"Bucket o nazwie {bucket_name} nie został znaleziony.")
        finally:
            response.close()

    def _get_upload_url(self) -> Tuple[str, str]:
        self._refresh_session_if_needed()
        get_upload_url: str = f'{self._api_url}/b2api/v2/b2_get_upload_url'
        upload_headers: Dict[str, str] = {'Authorization': self._auth_token}
        upload_data: Dict[str, str] = {"bucketId": self._bucket_id}

        response: requests.Response = self._session.post(get_upload_url, json=upload_data, headers=upload_headers)
        try:
            response.raise_for_status()
            data: Dict[str, Any] = response.json()
            return data['uploadUrl'], data['authorizationToken']
        finally:
            response.close()

    def upload_existing_file(
            self,
            file_path: str,
            file_name: Optional[str] = None
    ) -> Dict[str, Any]:
        self._refresh_session_if_needed()
        if not file_path.lower().endswith('.zip'):
            raise ValueError("File has to be .zip")

        if file_name is None:
            file_name = os.path.basename(file_path)

        with open(file_path, 'rb') as f:
            file_contents: bytes = f.read()

        file_sha1: str = hashlib.sha1(file_contents).hexdigest()
        upload_url, upload_auth_token = self._get_upload_url()
        file_headers: Dict[str, str] = {
            'Authorization': upload_auth_token,
            'X-Bz-File-Name': file_name,
            'Content-Type': 'b2/x-auto',
            'X-Bz-Content-Sha1': file_sha1,
        }

        response: requests.Response = self._session.post(upload_url, headers=file_headers, data=file_contents)
        try:
            response.raise_for_status()
            result: Dict[str, Any] = response.json()
            return result
        finally:
            response.close()

    def upload_zipped_jsoned_string(
            self,
            data: str,
            file_name: str
    ) -> None:
        self._refresh_session_if_needed()
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            zipf.writestr(f'{file_name}.json', data)
        zip_buffer.seek(0)

        bytes_to_be_sent = zip_buffer.getvalue()

        file_sha1: str = hashlib.sha1(bytes_to_be_sent).hexdigest()
        upload_url, upload_auth_token = self._get_upload_url()
        file_headers: Dict[str, str] = {
            'Authorization': upload_auth_token,
            'X-Bz-File-Name': f'{file_name}.zip',
            'Content-Type': 'b2/x-auto',
            'X-Bz-Content-Sha1': file_sha1,
        }

        response: requests.Response = self._session.post(upload_url, headers=file_headers, data=bytes_to_be_sent)
        try:
            response.raise_for_status()
        finally:
            response.close()

    def shutdown(self) -> None:
        self._session.close()