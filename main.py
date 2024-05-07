import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import json

from binance_archiver import DaemonManager


if __name__ == "__main__":

    load_dotenv()
    config_secret_name = os.environ.get('CONFIG_SECRET_NAME')
    blob_parameters_secret_name = os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY_SECRET_NAME')
    container_name_secret_name = os.environ.get('CONTAINER_NAME_SECRET_NAME')

    client = SecretClient(
        vault_url=os.environ.get('VAULT_URL'),
        credential=DefaultAzureCredential()
    )

    config = json.loads(client.get_secret(config_secret_name).value)
    azure_blob_parameters_with_key = client.get_secret(blob_parameters_secret_name).value
    container_name = client.get_secret(container_name_secret_name).value

    manager = DaemonManager(
        config=config,
        dump_path='temp',
        remove_csv_after_zip=True,
        remove_zip_after_upload=False,
        send_zip_to_blob=False,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        container_name=container_name
    )

    manager.run()
