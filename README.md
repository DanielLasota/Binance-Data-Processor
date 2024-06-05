Real-time market data listener and archiver in python. 
Saves raw binance data in zipped jsons on azure blob

Handles: 
spot, futures usd-m, futures coin-m
Level 2 orderbook deltas stream
transaction stream
orderbook snapshots with configured trigger interval 
24-hour WebSocket lifecycle. At the end of the WebSocket's lifespan, it initiates a new WebSocket to ensure the continuity of data flow is maintained seamlessly.

Configured to use contenerised on Azure with Azure blob and keyvault. 

![Zrzut ekranu 2024-06-02 230137](https://github.com/DanielLasota/Binance-Archiver/assets/127039319/b400f859-60ef-4995-936d-d68ecab82ddf)
