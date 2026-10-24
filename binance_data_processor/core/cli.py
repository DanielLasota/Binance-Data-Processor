from __future__ import annotations

import gc
import json
import pprint
import sys

from binance_data_processor.core.queue_pool import QueuePool
from binance_data_processor.core.setup_logger import setup_logger
from binance_data_processor.enums.data_sink_cfg import DataSinkCfg
from binance_data_processor.enums.cmd_reg_enum import CmdReg
from binance_data_processor.enums.market_enum import Market


class CLI:

    __slots__ = (
        'cfg',
        'queue_pool',
        'logger',
        'stop_callback'
    )

    def __init__(
            self,
            cfg: DataSinkCfg,
            queue_pool: QueuePool
    ):
        self.cfg = cfg
        self.queue_pool = queue_pool
        self.logger = setup_logger(dump=True)

    def handle_command(self, message):
        cmd, args = next(iter(message.items()))
        cmd = CmdReg(cmd)

        self.logger.info('\n')
        self.logger.info('################################################')
        self.logger.info('VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV')

        cmd_reg = {
            CmdReg.SHUTDOWN:
                lambda: self.shutdown(),
            CmdReg.MODIFY_SUBSCRIPTION:
                lambda: self.modify_subscription(action=args['type'], market=Market(args['market'].lower()), pair=args['asset'].upper()),
            CmdReg.OVERRIDE_CONFIG_INTERVAL:
                lambda: self.override_config_interval(selected_interval_name=args['selected_interval_name'], new_interval=args['new_interval']),
            CmdReg.SHOW_CONFIG:
                lambda: self.show_config(),
            CmdReg.SHOW_STATUS:
                lambda: self.show_status(),
            CmdReg.SHOW_IMPORTED_MODULES:
                lambda: self.show_imported_modules(),
            CmdReg.GC_COLLECT:
                lambda: self.gc_collect(),
        }

        command_executor = cmd_reg.get(cmd)
        if command_executor:
            return command_executor()
        else:
            raise Exception(f'Bad command, try again')

    def shutdown(self):
        ...
        # self.stop_callback()

    def modify_subscription(self, action: str, market: Market, pair: str):
        try:
            if action == 'subscribe':
                self.cfg.instruments.add_pair(market, pair)
            elif action == 'unsubscribe':
                self.cfg.instruments.remove_pair(market, pair)

            self.queue_pool.cmd_queue.put({'action': action, 'market': market, 'pair': pair})
            self.logger.info(f'{action}d {market} {pair}')

            out = {'requested': f'{action} {market} {pair}', 'cfg now': f'{self.cfg.instruments.get_pairs(market)}'}

            self.logger.info('^^^^^^^^^^^^')
            self.logger.info('############')
            self.logger.info('\n')

            return json.dumps(out)

        except Exception as e:
            self.logger.error(e)
            return e

    def override_config_interval(self, selected_interval_name: str, new_interval: int) -> str:

        self.cfg.time.update_interval(
            time_type=selected_interval_name,
            new_time=new_interval,
            logger=self.logger
        )

        out = {
            'requested': f'override {selected_interval_name}, new interval: {new_interval}',
            'actual_interval_time': f'{self.cfg.time}'
        }

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        return json.dumps(out)

    def show_config(self):
        self.logger.info("Configuration:\n%s", pprint.pformat(self.cfg, indent=1))

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')
        self.logger.info('\n')

        json_output = json.dumps(f'{self.cfg}')

        return json_output

    def show_status(self) -> str:
        output_lines = []

        output_lines.append("BINANCE ARCHIVER STATUS:")
        output_lines.append("------------------------------------------")
        output_lines.append("Queue Pool len:")
        output_lines.append("------------------------------------------")

        for (market, stream_type), queue_instance in self.queue_pool.queues.items():
            output_lines.append(f"{market} {stream_type} : {queue_instance.queue.qsize()}")
        output_lines.append("\n")

        output_lines.append("------------------------------------------")
        output_lines.append("Queue Pool newest message:")
        output_lines.append("------------------------------------------")

        for (market, stream_type), queue_instance in self.queue_pool.queues.items():
            try:
                last_message = queue_instance.queue.queue[-1] if queue_instance.queue.qsize() > 0 else "Empty queue"
            except Exception as e:
                last_message = f"Error: {e}"
            output_lines.append(f"{market} {stream_type} : {last_message}")
            output_lines.append(f"\n")

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
