from enum import Enum


class DataSaveTarget(Enum):
    JSON = "json"
    ZIP = "zip"
    BACKBLAZE = "backblaze"
    AZURE_BLOB = "azure_blob"
    LISTEN_ONLY_FOR_TEST_PURPOSES = "listen_only_for_test_purposes"
    ...
