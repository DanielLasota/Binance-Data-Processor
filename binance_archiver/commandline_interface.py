from __future__ import annotations

import logging

from binance_archiver.stream_service import StreamService
from binance_archiver.enum_.market_enum import Market


class CommandLineInterface:
    __slots__ = [
        'config',
        'instruments',
        'logger',
        'stream_service'
    ]

    def __init__(
        self,
        config: dict,
        logger: logging.Logger,
        stream_service: StreamService
    ):
        self.config = config
        self.instruments = config['instruments']
        self.logger = logger
        self.stream_service = stream_service

    def handle_command(self, message):
        command = list(message.items())[0][0]
        arguments = list(message.items())[0][1]

        if command == 'modify_subscription':
            self.modify_subscription(
                type_=arguments['type'],
                market=arguments['market'],
                asset=arguments['asset']
            )
        elif command == 'override_interval':
            self.modify_config_intervals(
                selected_interval_name=arguments['selected_interval_name'],
                new_interval=arguments['new_interval']
            )
        elif command == 'show_config':
            self.show_config()
        else:
            self.logger.warning('Bad command, try again')

    def modify_subscription(self, type_: str, market: str, asset: str):
        asset_upper = asset.upper()
        market_lower = market.lower()

        if type_ == 'subscribe':
            if asset_upper not in self.instruments[market_lower]:
                self.instruments[market_lower].append(asset_upper)
        elif type_ == 'unsubscribe':
            if asset_upper in self.instruments[market_lower]:
                self.instruments[market_lower].remove(asset_upper)

        self.stream_service.update_subscriptions(Market[market.upper()], asset_upper, type_)

    def modify_config_intervals(self, selected_interval_name: str, new_interval: int) -> None:

        if not isinstance(new_interval, int):
            self.logger.error(f'new_interval not an int!')
            return None

        if selected_interval_name in self.config:
            self.config[selected_interval_name] = new_interval
            self.logger.info(f"Updated {selected_interval_name} to {new_interval} seconds.")
        else:
            self.logger.warning(f"{selected_interval_name} not found in config.")

    def show_me_your_status(self):
        ...

    def show_config(self):
        self.logger.info(self.config)
