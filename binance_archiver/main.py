import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import json

from binance_archiver import launch_data_sink
from load_config import load_config


if __name__ == "__main__":

    # load_dotenv('C:/Users/defrg/archer.env')
    # config = load_config('almost_production_config.json')

    client = SecretClient(
        vault_url=os.environ.get('VAULT_URL'),
        credential=DefaultAzureCredential()
    )

    config_secret_name = os.environ.get('CONFIG_SECRET_NAME')

    blob_parameters_secret_name = os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY_SECRET_NAME')
    azure_container_name_secret_name = os.environ.get('AZURE_CONTAINER_NAME_SECRET_NAME')

    backblaze_access_key_id_secret_name = os.environ.get('BACKBLAZE_ACCESS_KEY_ID_BINANCE_PROD')
    backblaze_bucket_name_secret_name = os.environ.get('BACKBLAZE_BUCKET_NAME_BINANCE_PROD')
    backblaze_secret_access_key_secret_name = os.environ.get('BACKBLAZE_SECRET_ACCESS_KEY_BINANCE_PROD')
    backblaze_endpoint_url_secret_name = os.environ.get('BACKBLAZE_ENDPOINT_URL_BINANCE_PROD')

    config = json.loads(client.get_secret(config_secret_name).value)

    azure_blob_parameters_with_key = client.get_secret(blob_parameters_secret_name).value
    azure_container_name = client.get_secret(azure_container_name_secret_name).value

    backblaze_access_key_id = client.get_secret(backblaze_access_key_id_secret_name).value
    backblaze_secret_access_key = client.get_secret(backblaze_secret_access_key_secret_name).value
    backblaze_endpoint_url = client.get_secret(backblaze_endpoint_url_secret_name).value
    backblaze_bucket_name = client.get_secret(backblaze_bucket_name_secret_name).value

    data_sink = launch_data_sink(
        config,
        backblaze_access_key_id=backblaze_access_key_id,
        backblaze_secret_access_key=backblaze_secret_access_key,
        backblaze_endpoint_url=backblaze_endpoint_url,
        backblaze_bucket_name=backblaze_bucket_name
    )
