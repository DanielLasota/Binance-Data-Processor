import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import time
# import tracemalloc

from binance_data_processor import launch_data_sink, DataSinkConfig
from binance_data_processor.load_config import load_config_from_json


if __name__ == "__main__":

    load_dotenv('binance-archiver-2.env')
    config_from_json = load_config_from_json('almost_production_config.json')

    client = SecretClient(
        vault_url=os.environ.get('VAULT_URL'),
        credential=DefaultAzureCredential()
    )

    # config = json.loads(client.get_secret('archer-main-config').value)
    backblaze_access_key_id = client.get_secret('backblaze-access-key-id-binance-prod').value
    backblaze_secret_access_key = client.get_secret('backblaze-secret-access-key-binance-prod').value
    backblaze_endpoint_url = client.get_secret('backblaze-endpoint-url-binance-prod').value
    backblaze_bucket_name = client.get_secret('backblaze-bucket-name-binance-prod').value

    data_sink_config = DataSinkConfig(
        instruments={
            'spot': config_from_json['instruments']['spot'],
            'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
            'coin_m_futures': config_from_json['instruments']['coin_m_futures']
        },
        time_settings={
            "file_duration_seconds": config_from_json["file_duration_seconds"],
            "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
            "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
        },
        data_save_target=config_from_json['data_save_target']
    )

    # tracemalloc.start()
    client.close()

    data_sink = launch_data_sink(
        data_sink_config=data_sink_config
    )

    while not data_sink.global_shutdown_flag.is_set():
        time.sleep(8)

    data_sink.logger.info('the program has ended, exiting')
