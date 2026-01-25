from __future__ import annotations

import gc
import json
import logging
import pprint
import sys
import threading

from binance_data_processor.core.stream_service import StreamService
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg
from binance_data_processor.enums.commands_registry_enum import CommandsRegistry
from binance_data_processor.enums.market_enum import Market


class CommandLineInterface:
    __slots__ = [
        'stream_service',
        'data_sink_config',
        'logger',
        'stop_callback'
    ]

    def __init__(
            self,
            stream_service: StreamService,
            cfg: DataSinkCfg,
            stop_callback
    ):
        self.stream_service = stream_service
        self.data_sink_config = cfg
        self.logger = logging.getLogger('binance_data_sink')
        self.stop_callback = stop_callback

    def handle_command(
            self,
            message
    ):
        command, arguments = next(iter(message.items()))
        command = CommandsRegistry(command)

        self.logger.info('\n')
        self.logger.info('################################################')
        self.logger.info('VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV')

        commands_registry = {
            CommandsRegistry.SHUTDOWN:
                lambda: self.shutdown(),
            CommandsRegistry.MODIFY_SUBSCRIPTION:
                lambda: self.modify_subscription(type_=arguments['type'], market=Market(arguments['market'].lower()), instrument=arguments['asset'].upper()),
            CommandsRegistry.OVERRIDE_CONFIG_INTERVAL:
                lambda: self.override_config_interval(selected_interval_name=arguments['selected_interval_name'], new_interval=arguments['new_interval']),
            CommandsRegistry.SHOW_CONFIG:
                lambda: self.show_config(),
            CommandsRegistry.SHOW_STATUS:
                lambda: self.show_status(),
            CommandsRegistry.SHOW_IMPORTED_MODULES:
                lambda: self.show_imported_modules(),
            CommandsRegistry.GC_COLLECT:
                lambda: self.gc_collect(),
        }

        command_executor = commands_registry.get(command)
        if command_executor:
            return command_executor()
        else:
            raise Exception(f'Bad command, try again')

    def shutdown(self):
        self.stop_callback()

    def modify_subscription(
            self,
            type_: str,
            market: Market,
            instrument: str
    ):
        try:

            if type_ == 'subscribe':
                self.data_sink_config.instruments.add_pair(market=market, pair=instrument)
            elif type_ == 'unsubscribe':
                self.data_sink_config.instruments.remove_pair(market=market, instrument=instrument)

            self.stream_service.update_subscriptions(
                market=market,
                asset_upper=instrument,
                action=type_
            )

            self.logger.info(f'{type_}d {market} {instrument}')

            final_output = {
                'requested': f'{type_} {market} {instrument}',
                'actual_instruments': f'{self.stream_service.cfg.instruments.get_pairs(market=market)}'
            }

            self.logger.info('^^^^^^^^^^^^')
            self.logger.info('############')
            self.logger.info('\n')

            return json.dumps(final_output)

        except Exception as e:
            self.logger.error(e)
            return e

    def override_config_interval(
            self,
            selected_interval_name: str,
            new_interval: int
    ) -> str:

        self.data_sink_config.time_cfg.update_interval(
            time_type=selected_interval_name,
            new_time=new_interval,
            logger=self.logger
        )

        final_output = {
            'requested': f'override {selected_interval_name}, new interval: {new_interval}',
            'actual_interval_time': f'{self.stream_service.cfg.time_cfg}'
        }

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        return json.dumps(final_output)

    def show_config(self):
        self.logger.info("Configuration:\n%s", pprint.pformat(self.data_sink_config, indent=1))

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        json_output = json.dumps(f'{self.data_sink_config}')

        return json_output

    def show_status(self) -> str:
        output_lines = []

        output_lines.append("BINANCE ARCHIVER STATUS:")
        output_lines.append("------------------------------------------")
        output_lines.append("Queue Pool len:")
        output_lines.append("------------------------------------------")

        for (market, stream_type), queue_instance in self.stream_service.queue_pool.queues.items():
            output_lines.append(f"{market} {stream_type} : {queue_instance.queue.qsize()}")
        output_lines.append("\n")


        output_lines.append("------------------------------------------")
        output_lines.append("Queue Pool newest message:")
        output_lines.append("------------------------------------------")

        for (market, stream_type), queue_instance in self.stream_service.queue_pool.queues.items():
            try:
                last_message = queue_instance.queue.queue[-1] if queue_instance.queue.qsize() > 0 else "Empty queue"
            except Exception as e:
                last_message = f"Error: {e}"
            output_lines.append(f"{market} {stream_type} : {last_message}")
            output_lines.append(f"\n")

        output_lines.append("------------------------------------------")
        output_lines.append("Stream service status:")
        output_lines.append("------------------------------------------")

        dict_of_stream_listeners = self.stream_service.get_stream_listeners_status()
        output_lines.append(pprint.pformat(dict_of_stream_listeners))

        output_lines.append("------------------------------------------")
        output_lines.append("Threads status:")
        output_lines.append("------------------------------------------")

        threads = [thread for thread in threading.enumerate() if thread.is_alive()]
        output_lines.append("\n ".join(str(thread) for thread in threads))

        output_lines.append("------------------------------------------")
        output_lines.append("Loaded Modules:")
        output_lines.append("------------------------------------------")

        loaded_modules = [module for module in sys.modules.keys()]

        output_lines.append("\n ".join(str(module) for module in loaded_modules))

        final_output = "\n".join(output_lines)
        self.logger.info(final_output)

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        return final_output

    def show_imported_modules(self):
        modules = list(sys.modules.keys())
        self.logger.info('returned list of imported modules check api')
        return json.dumps(modules)

    def gc_collect(self) -> str:
        self.logger.info('invocating gc.collect()')
        gc.collect()

        self.logger.info('ended gc.collect()')
        return 'gc_collected'

    def show_jsoned_status(self) -> str:

        def convert_keys_to_str(obj):
            if isinstance(obj, dict):
                return {str(k): convert_keys_to_str(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_keys_to_str(item) for item in obj]
            else:
                return obj

        status = {}

        status["title"] = "BINANCE ARCHIVER STATUS"

        queue_status = []
        for (market, stream_type), queue_instance in self.stream_service.queue_pool.queues.items():
            try:
                last_message = queue_instance.queue.queue[-1] if queue_instance.queue.qsize() > 0 else "Empty queue"
            except Exception as e:
                last_message = f"Error: {e}"
            queue_status.append({
                "market": str(market),
                "stream_type": str(stream_type),
                "queue_size": queue_instance.queue.qsize(),
                "last_message": last_message
            })
        status["queue_status"] = queue_status

        dict_of_stream_listeners = self.stream_service.get_stream_listeners_status()
        status["stream_service_status"] = convert_keys_to_str(dict_of_stream_listeners)

        threads = [thread for thread in threading.enumerate() if thread.is_alive()]
        threads_info = []
        for t in threads:
            threads_info.append({
                "name": t.name,
                "ident": t.ident,
                "daemon": t.daemon,
            })
        status["threads_status"] = threads_info

        json_output = json.dumps(status, indent=2)
        self.logger.info(json_output)
        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        return json_output
