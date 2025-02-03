from __future__ import annotations

import logging
import pprint

# import tracemalloc #
# import objgraph #
# from pympler import asizeof, muppy #

import binance_archiver.data_sink_facade
from binance_archiver import DataSinkConfig
from binance_archiver.stream_service import StreamService
from binance_archiver.enum_.market_enum import Market


class CommandLineInterface:
    __slots__ = [
        'stream_service',
        'data_sink_config',
        'logger',
    ]

    def __init__(
            self,
            stream_service: StreamService,
            data_sink_config: DataSinkConfig
    ):
        self.stream_service = stream_service
        self.data_sink_config = data_sink_config
        self.logger = logging.getLogger('binance_data_sink')

    def handle_command(
            self,
            message
    ) -> None:
        command = list(message.items())[0][0]
        arguments = list(message.items())[0][1]

        self.logger.info('\n')
        self.logger.info('############')
        self.logger.info('VVVVVVVVVVVV')

        if command == 'modify_subscription':
            self.modify_subscription(
                type_=arguments['type'],
                market=Market(arguments['market'].lower()),
                instrument=arguments['asset'].upper()
            )

        elif command == 'override_interval':
            self.modify_config_intervals(
                selected_interval_name=arguments['selected_interval_name'],
                new_interval=arguments['new_interval']
            )

        elif command == 'show_config':
            self.show_config()

        elif command == 'show_tracemalloc_snapshot_statistics':
            self.show_tracemalloc_snapshot_statistics()

        elif command == 'show_objgraph_growth':
            self.show_objgraph_growth()

        elif command == 'show_pympler_all_objects_analysis':
            self.show_pympler_all_objects_analysis()

        elif command == 'show_pympler_data_sink_object_analysis':
            self.show_pympler_data_sink_object_analysis()

        elif command == 'show_pympler_data_sink_object_analysis_with_detail_level':
            self.show_pympler_data_sink_object_analysis_with_detail_level(n_detail_level=arguments['n_detail_level'])

        elif command == 'show_pympler_data_sink_object_analysis_with_manual_iteration':
            self.show_pympler_data_sink_object_analysis_with_manual_iteration()

        else:
            self.logger.warning('Bad command, try again')

        self.logger.info('^^^^^^^^^^^^')
        self.logger.info('############')

    def modify_subscription(
            self,
            type_: str,
            market: Market,
            instrument: str
    ) -> None:

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

    def modify_config_intervals(
            self,
            selected_interval_name: str,
            new_interval: int
    ) -> None:

        self.data_sink_config.time_settings.update_interval(
            setting_name=selected_interval_name,
            new_time=new_interval,
            logger=self.logger
        )

    def show_config(self):
        self.logger.info("Configuration:\n%s", pprint.pformat(self.data_sink_config, indent=1))

    def show_tracemalloc_snapshot_statistics(self):
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        self.logger.info("[ Top 40 ]")
        for stat in top_stats[:40]:
            self.logger.info(stat)

    def show_objgraph_growth(self):
        objgraph.show_growth(limit=8)
        # self.logger.info(objgraph.growth(limit=1))

        try:
            object_type_with_largest_growth = objgraph.growth(limit=1)[0][0]
            objgraph.show_backrefs(
                object_type_with_largest_growth,
                filename="backrefs.png"
            )
        except Exception as e:
            self.logger.error(e)

    def show_pympler_all_objects_analysis(self):

        # self.logger.info('pympler 1')

        all_objects = muppy.get_objects()
        # self.logger.info('pympler 2')

        objects_with_sizes = []
        # self.logger.info('pympler 3')

        for obj in all_objects:
            # self.logger.info('pympler 4')

            try:
                # self.logger.info('pympler 5')

                size = asizeof.asizeof(obj)
                # self.logger.info('pympler 6')

                objects_with_sizes.append((size, type(obj).__name__, repr(obj)[:100]))

            except (TypeError, RecursionError) as e:
                self.logger.info(e)

        objects_with_sizes.sort(reverse=True, key=lambda x: x[0])

        self.logger.info("biggest objects in memory:")
        for size, obj_type, obj_repr in objects_with_sizes[:10]:
            self.logger.info(
                f"Size: {size} bytes ({size / (1024 * 1024):.2f} MB), Type: {obj_type}, object: {obj_repr}"
            )

    def show_pympler_data_sink_object_analysis(self):
        data_sink_objects = [obj for obj in muppy.get_objects()
                             if isinstance(obj, binance_archiver.data_sink_facade.BinanceDataSink)]

        if len(data_sink_objects) == 1:
            data_sink = data_sink_objects[0]
            total_size = asizeof.asizeof(data_sink)
            self.logger.info(f"Total size of data_sink: {total_size} bytes ({total_size / (1024 * 1024):.2f} MB)")
        else:
            self.logger.info(f'len of data sink type object list: {len(data_sink_objects)}')

    def show_pympler_data_sink_object_analysis_with_detail_level(self, n_detail_level):
        data_sink_objects = [obj for obj in muppy.get_objects()
                             if isinstance(obj, binance_archiver.data_sink_facade.BinanceDataSink)]

        if len(data_sink_objects) == 1:
            data_sink = data_sink_objects[0]
            detailed_size = asizeof.asized(data_sink, detail=n_detail_level)
            self.logger.info(f"Total size of data_sink: {detailed_size.size} bytes "
                             f"({detailed_size.size / (1024 * 1024):.2f} MB)")
            self.logger.info("Detailed analysis:")
            self.logger.info(detailed_size.format())
        else:
            self.logger.info(f'len of data sink type object list: {len(data_sink_objects)}')

    def show_pympler_data_sink_object_analysis_with_manual_iteration(self):
        data_sink_objects = [obj for obj in muppy.get_objects()
                             if isinstance(obj, binance_archiver.data_sink_facade.BinanceDataSink)]

        if len(data_sink_objects) != 1:
            raise Exception('len of data_sink_objects != 1')

        data_sink: object = data_sink_objects[0]

        total_size = asizeof.asizeof(data_sink)
        self.logger.info(f"full data_sink size:{total_size} bytes ({total_size / (1024 * 1024):.2f} MB)")

        detailed_size = asizeof.asized(data_sink, detail=3)
        self.logger.info("deep data_sink_analysis::")
        self.logger.info(detailed_size.format())

        self.logger.info("Detailed sizes of data_sink attributes::")
        for attr_name in dir(data_sink):
            if attr_name.startswith('__') and attr_name.endswith('__'):
                continue
            attr_value = getattr(data_sink, attr_name)
            attr_size = asizeof.asizeof(attr_value)
            self.logger.info(f"Attribute '{attr_name}': size {attr_size} bytes "
                             f"({attr_size / (1024 * 1024):.2f} MB)"
                             f", type: {type(attr_value).__name__}")

            if hasattr(attr_value, '__dict__'):
                sub_attrs = vars(attr_value)
                for sub_attr_name, sub_attr_value in sub_attrs.items():
                    sub_attr_size = asizeof.asizeof(sub_attr_value)
                    self.logger.info(
                        f"sub-attribute '{sub_attr_name}': size {sub_attr_size} bytes "
                        f"({sub_attr_size / (1024 * 1024):.2f} MB)"
                        f", type {type(sub_attr_value).__name__}")
