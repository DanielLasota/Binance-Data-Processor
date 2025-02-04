# Real-time market data listener and archiver in python. 
Saves raw binance data in zipped jsons on azure blob

# Handles: 
spot, futures usd-m, futures coin-m
Level 2 orderbook difference depth stream
trade stream
orderbook snapshots with configured trigger interval 
24-hour WebSocket lifecycle. At the end of the WebSocket's lifespan, it initiates a new WebSocket to ensure the continuity of data flow is maintained seamlessly.

Configured to use contenerised on Azure with Azure blob and keyvault

![image](https://github.com/user-attachments/assets/a9461c8d-b5a7-43de-b1cc-96ef5df72f40)

![image](https://github.com/user-attachments/assets/93a9cece-21fd-406c-8555-fbb774188265)

![Zrzut ekranu 2024-06-02 230137](https://github.com/DanielLasota/Binance-Archiver/assets/127039319/b400f859-60ef-4995-936d-d68ecab82ddf)



## Installation

```bash
# to be announced
```

## Usage

import the `run_stonks_analysis` function from the `stonks` module and run the script:

```python
import time
from dotenv import load_dotenv

from binance_archiver import load_config_from_json, DataSinkConfig, launch_data_sink

if __name__ == "__main__":

    load_dotenv('binance-archiver.env')

    data_sink_config = DataSinkConfig(
        instruments={
            'spot': ['BTCUSDT', 'XRPUSDT'],
            'usd_m_futures': ['BTCUSDT', 'XRPUSDT'],
            'coin_m_futures': ['BTCUSDT', 'XRPUSDT']
        },
        time_settings={
            "file_duration_seconds": 120,
            "snapshot_fetcher_interval_seconds": 300,
            "websocket_life_time_seconds": 120
        },
        data_save_target='azure_blob'
    )

    data_sink = launch_data_sink(data_sink_config=data_sink_config)
    
    time.sleep(99)
    
    data_sink.shutdown()
    

```
