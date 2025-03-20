# Binance Data Processor
## A Package for Listening / Archiving to Multiple Binance WebSockets with Snapshots.
## Package contains scraper with built-in quality report for each column

```bash
pip install binance-data-processor
```

### Functions:
1. Listener
2. Archiver 
3. Scraper  
4. CSV Data Quality Checker

### Handles:
spot  
futures usd-m  
futures coin-m

difference depth stream  
trade stream  
depth snapshot  

.json, zip, azure, backblaze




## Listener Mode:

```python
from binance_data_processor import launch_data_listener


class SampleObserverClass:
    @staticmethod
    def update(message):
        print(f"message: {message}")


if __name__ == '__main__':
    sample_observer = SampleObserverClass()

    data_listener = launch_data_listener(observers=[sample_observer])

```

## Archiver Mode: 

backblaze / azure env pattern:

VAULT_URL=  
BACKBLAZE_ACCESS_KEY_ID=  
BACKBLAZE_SECRET_ACCESS_KEY=  
BACKBLAZE_ENDPOINT_URL=  
BACKBLAZE_BUCKET_NAME=

```python
import os
import time
from dotenv import load_dotenv
from binance_data_processor import launch_data_sink

env_path = os.path.join(os.path.expanduser('~'), 'Documents/sample_env.env')
load_dotenv(env_path)

if __name__ == "__main__":

    data_sink = launch_data_sink()

    while not data_sink.global_shutdown_flag.is_set():
        time.sleep(16)

    data_sink.logger.info('the program has ended, exiting')

```
now create your own config:

```python
import os
import time
from dotenv import load_dotenv

from binance_data_processor import DataSinkConfig, launch_data_sink, StorageConnectionParameters

env_path = os.path.join(os.path.expanduser('~'), 'Documents/sample_env.env')
load_dotenv(env_path)

if __name__ == "__main__":
    data_sink_config = DataSinkConfig(
        instruments={
            'spot': ['BTCUSDT', 'TRXUSDT'],
            'usd_m_futures': ['BTCUSDT', 'TRXUSDT'],
            'coin_m_futures': ['BTCUSD_PERP', 'TRXUSD_PERP']
        },
        time_settings={
            "file_duration_seconds": 300,
            "snapshot_fetcher_interval_seconds": 60,
            "websocket_life_time_seconds": 60 * 60 * 23
        },
        data_save_target='azure_blob',
        storage_connection_parameters=StorageConnectionParameters()
    )

    data_sink = launch_data_sink(data_sink_config=data_sink_config)

    while not data_sink.global_shutdown_flag.is_set():
        time.sleep(16)

    data_sink.logger.info('the program has ended, exiting')
```
## Scraper with quality check:

```python
import os

from dotenv import load_dotenv

from binance_data_processor import download_csv_data, StorageConnectionParameters

env_path = os.path.join(os.path.expanduser('~'), 'Documents/sample_env.env')
load_dotenv(env_path)

if __name__ == '__main__':
    download_csv_data(
        date_range=['11-03-2025', '12-03-2025'],
        storage_connection_parameters=StorageConnectionParameters(),
        pairs=[
            'BTCUSDT',
            'TRXUSDT'
        ],
        markets=[
            'SPOT',
            'USD_M_FUTURES',
            'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            'DEPTH_SNAPSHOT',
        ],
        skip_existing=False,
        amount_of_files_to_be_downloaded_at_once=20
    )

```
Check csvs with certificate:

```python
from binance_data_processor import conduct_data_quality_analysis_on_whole_directory, conduct_data_quality_analysis_on_specified_csv_list

if __name__ == '__main__':
    conduct_data_quality_analysis_on_specified_csv_list(
        csv_paths=[
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_spot_btcusdt_10-03-2025.csv',
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_usd_m_futures_btcusdt_10-03-2025.csv',
            'C:/Users/daniel/Documents/binance_archival_data/binance_depth_snapshot_coin_m_futures_btcusd_perp_10-03-2025.csv',
        ]
    )

    conduct_data_quality_analysis_on_whole_directory('C:/Users/daniel/Documents/binance_archival_data/')

```  

### sample screenshots:
![image](https://github.com/user-attachments/assets/a9461c8d-b5a7-43de-b1cc-96ef5df72f40)

![image](https://github.com/user-attachments/assets/93a9cece-21fd-406c-8555-fbb774188265)

![Zrzut ekranu 2024-06-02 230137](https://github.com/DanielLasota/Binance-Archiver/assets/127039319/b400f859-60ef-4995-936d-d68ecab82ddf)

![Control-V (4)](https://github.com/user-attachments/assets/5917b44c-e545-46f5-b5d0-3b3f5d322bb2)

