import re
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import threading

from binance_archiver.orderbook_level_2_listener.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_listener import StreamListener
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType


class TestArchiverDaemon:

    def test_given_pairs_is_url_correct(self):

        _queue = DifferenceDepthQueue()

        stream_listener = StreamListener(queue=_queue,
                                         pairs=['BTCUSDT', 'ETHUSDT'],
                                         stream_type=StreamType.DIFFERENCE_DEPTH,
                                         market=Market.SPOT
                                         )

        print(stream_listener.websocket_app.url)
        assert (stream_listener.websocket_app.url ==
                'wss://stream.binance.com:443/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms')

    def test_given_new_websocket_app_is_connection_established(self):

        _queue = DifferenceDepthQueue()

        stream_listener = StreamListener(queue=_queue,
                                         pairs=['BTCUSDT', 'ETHUSDT'],
                                         stream_type=StreamType.DIFFERENCE_DEPTH,
                                         market=Market.SPOT
                                         )

        _queue.currently_accepted_stream_id = stream_listener.id.id
        thread = threading.Thread(target=stream_listener.websocket_app.run_forever)
        thread.daemon = True
        thread.start()

        time.sleep(5)

        stream_listener.websocket_app.close()

        time.sleep(2)

        assert thread.is_alive() is False
        assert _queue.qsize() > 0
        assert not stream_listener.websocket_app.keep_running
        assert stream_listener.websocket_app.sock is None
