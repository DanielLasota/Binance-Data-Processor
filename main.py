import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import json
import pprint

from binance_archiver import DaemonManager

if __name__ == "__main__":
    load_dotenv('C:/Users/daniellasota/archer.env')
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

    config = {
            "daemons": {
                "markets": {

                    "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT",
                             "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
                                      "LTCUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP",
                                       "DOGEUSD_PERP", "ADAUSD_PERP", "LTCUSD_PERP", "AVAXUSD_PERP", "TRXUSD_PERP",
                                       "DOTUSD_PERP"]
                },
                "file_duration_seconds": 300,
                "snapshot_fetcher_interval_seconds": 300,
                "websocket_life_time_seconds": 600,
                "websocket_overlap_seconds": 20,
                "save_to_json": False,
                "save_to_zip": False,
                "send_zip_to_blob": True
            }
        }

    config = \
        {
            "daemons": {
                "markets": {

                    "spot": ["BTCUSDT", "ETHUSDT"],

                    "usd_m_futures": ["BTCUSDT", "ETHUSDT"],

                    "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP"]
                },
                "file_duration_seconds": 60,
                "snapshot_fetcher_interval_seconds": 30,
                "websocket_life_time_seconds": 60,
                "websocket_overlap_seconds": 20,
                "save_to_json": True,
                "save_to_zip": False,
                "send_zip_to_blob": False
            }
        }

    manager = DaemonManager(
        config=config,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        container_name=container_name
    )

    manager.run()

'''
ContainerAppConsoleLogs_CL
| where TimeGenerated >= ago(7d)
| project TimeGenerated, Log_s
| sort by TimeGenerated desc
'''

