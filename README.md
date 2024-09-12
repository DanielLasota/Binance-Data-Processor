# Real-time market data listener and archiver in python. 
Saves raw binance data in zipped jsons on azure blob

# Handles: 
spot, futures usd-m, futures coin-m
Level 2 orderbook deltas stream
trade stream
orderbook snapshots with configured trigger interval 
24-hour WebSocket lifecycle. At the end of the WebSocket's lifespan, it initiates a new WebSocket to ensure the continuity of data flow is maintained seamlessly.

Configured to use contenerised on Azure with Azure blob and keyvault

![Zrzut ekranu 2024-06-02 230137](https://github.com/DanielLasota/Binance-Archiver/assets/127039319/b400f859-60ef-4995-936d-d68ecab82ddf)

## Installation

```bash
# to be announced
```

## Usage

import the `run_stonks_analysis` function from the `stonks` module and run the script:

```python
from binance_archiver.orderbook_level_2_listener.archiver_daemon import launch_data_sink

if __name__ == '__main__':

    config = {
        "instruments": {
            "spot": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOTUSDT"],
            "usd_mfutures": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOTUSDT"],
            "coin_m_futures": ["BTCUSD_PERP", "ETHUSD_PERP", "BNBUSD_PERP", "SOLUSD_PERP", "XRPUSD_PERP", "DOTUSD_PERP"]
        },
        "file_duration_seconds": 300,
        "snapshot_fetcher_interval_seconds": 300,
        "websocket_life_time_seconds": 600,
        "save_to_json": False,
        "save_to_zip": False,
        "send_zip_to_blob": False
    }

    azure_blob_parameters_with_key = 'some azure blob parameters with key'
    container_name = 'some-azure-container-name'

    data_sink = launch_data_sink(
        config,
        azure_blob_parameters_with_key=azure_blob_parameters_with_key,
        container_name=container_name
    )

    time.sleep(2115)
    data_sink.shutdown()

```
