from __future__ import annotations

import logging
import threading
import time
import traceback

from binance_archiver import DataSinkConfig
from binance_archiver.enum_.asset_parameters import AssetParameters
from binance_archiver.queue_pool import ListenerQueuePool, DataSinkQueuePool
from binance_archiver.difference_depth_queue import DifferenceDepthQueue
from binance_archiver.enum_.market_enum import Market
from binance_archiver.enum_.stream_type_enum import StreamType
from binance_archiver.stream_listener import StreamListener
from binance_archiver.trade_queue import TradeQueue


class StreamService:

    __slots__ = [
        'logger',
        'queue_pool',
        'global_shutdown_flag',
        'data_sink_config',
        'is_someone_overlapping_right_now_flag',
        'stream_listeners',
        'overlap_lock'
    ]

    def __init__(
        self,
        queue_pool: DataSinkQueuePool | ListenerQueuePool,
        global_shutdown_flag: threading.Event,
        data_sink_config: DataSinkConfig
    ):
        self.logger = logging.getLogger('binance_data_sink')
        self.queue_pool = queue_pool
        self.data_sink_config = data_sink_config
        self.global_shutdown_flag = global_shutdown_flag
        self.is_someone_overlapping_right_now_flag = threading.Event()
        self.stream_listeners: dict[tuple[Market, StreamType, str], StreamListener | None] = {}
        self.overlap_lock: threading.Lock = threading.Lock()

    def run(self):
        for market, instruments in self.data_sink_config.instruments.dict.items():
            for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
                asset_parameters = AssetParameters(
                    market=market,
                    stream_type=stream_type,
                    pairs=self.data_sink_config.instruments.get_pairs(market=market)
                )
                self.start_stream_service(
                    queue=self.queue_pool.get_queue(market, stream_type),
                    asset_parameters=asset_parameters
                )

    def start_stream_service(
            self,
            queue: DifferenceDepthQueue | TradeQueue,
            asset_parameters: AssetParameters
    ) -> None:

        thread = threading.Thread(
            target=self._stream_service,
            args=(
                queue,
                asset_parameters
            ),
            name=f'stream_service: market: {asset_parameters.market}, stream_type: {asset_parameters.stream_type}'
        )
        thread.start()

    def _stream_service(
        self,
        queue: DifferenceDepthQueue | TradeQueue,
        asset_parameters: AssetParameters
    ) -> None:

        def sleep_with_flag_check(duration) -> None:
            interval = 1
            for _ in range(0, duration, interval):
                if self.global_shutdown_flag.is_set():
                    break
                time.sleep(interval)

        while not self.global_shutdown_flag.is_set():
            new_stream_listener = None
            old_stream_listener = None

            try:
                old_stream_listener = StreamListener(
                    queue=queue,
                    asset_parameters=asset_parameters
                )
                self.stream_listeners[(asset_parameters.market, asset_parameters.stream_type, 'old')] = old_stream_listener

                if asset_parameters.stream_type is StreamType.DIFFERENCE_DEPTH_STREAM:
                    queue.currently_accepted_stream_id = old_stream_listener.id.id
                elif asset_parameters.stream_type is StreamType.TRADE_STREAM:
                    queue.currently_accepted_stream_id = old_stream_listener.id

                old_stream_listener.start_websocket_app()
                new_stream_listener = None

                while not self.global_shutdown_flag.is_set():
                    sleep_with_flag_check(self.data_sink_config.time_settings.websocket_life_time_seconds)

                    while self.is_someone_overlapping_right_now_flag.is_set():
                        time.sleep(1)

                    if self.global_shutdown_flag.is_set():
                        break

                    with self.overlap_lock:

                        self.is_someone_overlapping_right_now_flag.set()
                        self.logger.info(
                            f'{asset_parameters.market} {asset_parameters.stream_type} {old_stream_listener.id.start_timestamp} started changing ws')

                        new_stream_listener = StreamListener(
                            queue=queue,
                            asset_parameters=asset_parameters
                        )

                        new_stream_listener.start_websocket_app()
                        self.stream_listeners[(asset_parameters.market, asset_parameters.stream_type, 'new')] = new_stream_listener

                    while not queue.did_websockets_switch_successfully and not self.global_shutdown_flag.is_set():
                        time.sleep(1)

                    with self.overlap_lock:
                        self.is_someone_overlapping_right_now_flag.clear()
                    self.logger.info(f"{asset_parameters.market} {asset_parameters.stream_type} {old_stream_listener.id.start_timestamp} overlapped")

                    if not self.global_shutdown_flag.is_set():
                        queue.did_websockets_switch_successfully = False

                        old_stream_listener.close_websocket_app()
                        old_stream_listener = new_stream_listener

                        self.stream_listeners[(asset_parameters.market, asset_parameters.stream_type, 'new')] = None
                        self.stream_listeners[(asset_parameters.market, asset_parameters.stream_type, 'old')] = old_stream_listener

            except Exception as e:
                self.logger.error(f'{e}, something bad happened')
                self.logger.error("Traceback (most recent call last):")
                self.logger.error(traceback.format_exc())

            finally:
                for listener in (old_stream_listener, new_stream_listener):
                    if listener and listener._ws and listener._ws.state in [0, 1]:
                        listener.close_websocket_app()

    def update_subscriptions(
            self,
            market: Market,
            asset_upper: str,
            action: str
    ) -> None:
        for stream_type in [StreamType.DIFFERENCE_DEPTH_STREAM, StreamType.TRADE_STREAM]:
            for status in ['old', 'new']:
                stream_listener: StreamListener = self.stream_listeners.get((market, stream_type, status))
                if stream_listener:
                    stream_listener.change_subscription(action=action, pair=asset_upper)

    def get_stream_listeners_status(self) -> dict[tuple[Market, StreamType, str], str]:

        statuses: dict[tuple[Market, StreamType, str], str] = {}

        for key, listener in self.stream_listeners.items():
            if listener is None:
                statuses[key] = "Listener not initialized"
            else:
                if listener._ws is None:
                    statuses[key] = "WebSocket not connected"
                else:
                    state = listener._ws.state
                    if state == 0:
                        statuses[key] = "Connecting"
                    elif state == 1:
                        statuses[key] = "Connected"
                    elif state == 2:
                        statuses[key] = "Closing"
                    elif state == 3:
                        statuses[key] = "Closed"
                    else:
                        statuses[key] = "Unknown state"
        return statuses
