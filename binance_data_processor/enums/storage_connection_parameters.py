from dataclasses import dataclass
import os


@dataclass(slots=True)
class BlobConnParams:
    backblaze_access_key_id: str | None = None
    backblaze_secret_access_key: str | None = None
    backblaze_endpoint_url: str | None = None
    backblaze_bucket_name: str | None = None

    def __post_init__(self):
        if self.backblaze_access_key_id is None:
            self.backblaze_access_key_id = os.environ.get('BACKBLAZE_ACCESS_KEY_ID')
        if self.backblaze_secret_access_key is None:
            self.backblaze_secret_access_key = os.environ.get('BACKBLAZE_SECRET_ACCESS_KEY')
        if self.backblaze_endpoint_url is None:
            self.backblaze_endpoint_url = os.environ.get('BACKBLAZE_ENDPOINT_URL')
        if self.backblaze_bucket_name is None:
            self.backblaze_bucket_name = os.environ.get('BACKBLAZE_BUCKET_NAME')
