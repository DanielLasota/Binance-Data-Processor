import time
import json
import pytest

from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId
from binance_archiver.orderbook_level_2_listener.trade_queue import TradeQueue, ClassInstancesAmountLimitException


def format_message_string_that_is_pretty_to_binance_string_format(message: str) -> str:
    message = message.strip()
    data = json.loads(message)
    compact_message = json.dumps(data, separators=(',', ':'))

    return compact_message


class TestTradeQueue:

    # TradeQueue singleton init tests
    #
    def test_given_too_many_difference_depth_queue_instances_exists_when_creating_new_then_exception_is_thrown(self):
        for _ in range(4):
            TradeQueue(Market.SPOT)

        with pytest.raises(ClassInstancesAmountLimitException):
            TradeQueue(Market.SPOT)

        TradeQueue.clear_instances()

    def test_given_checking_amount_of_instances_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = TradeQueue.get_instance_count()
        assert instance_count == 0

        for _ in range(4):
            TradeQueue(Market.SPOT)

        assert TradeQueue.get_instance_count() == 4

        TradeQueue.clear_instances()

    def test_given_instances_amount_counter_reset_when_clear_instances_method_invocation_then_amount_is_zero(self):
        for _ in range(4):
            TradeQueue(Market.SPOT)

        TradeQueue.clear_instances()

        assert TradeQueue.get_instance_count() == 0
        TradeQueue.clear_instances()

    # _put_with_no_repetitions tests
    #

    def test_given_putting_message_when_putting_message_of_currently_accepted_stream_id_then_message_is_being_added_to_the_queue(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id
        mocked_timestamp_of_receive = 2115

        _first_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''
        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_first_listener_message_1)

        difference_depth_queue.put_trade_message(
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())
        assert (_first_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert len(difference_depth_queue_content_list) == 1

        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_putting_message_from_no_longer_accepted_stream_listener_id_when_try_to_put_then_message_is_not_added_to_the_queue(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869318,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_4 = '''
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869319,
                    "s": "DOTUSDT",
                    "U": 7871863948,
                    "u": 7871863950,
                    "b": [
                        [
                            "7.19800000",
                            "1817.61000000"
                        ],
                        [
                            "7.19300000",
                            "1593.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "7.20800000",
                            "1911.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)
        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_4)
        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_4)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=2115
        )

        assert difference_depth_queue.currently_accepted_stream_id == new_stream_listener_id.id
        assert difference_depth_queue.qsize() == 4

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert (_old_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_2, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_3, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_new_listener_message_4, mocked_timestamp_of_receive) in difference_depth_queue_content_list

        assert (_new_listener_message_1, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_2, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_3, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_old_listener_message_4, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        TradeQueue.clear_instances()

    @pytest.mark.skip
    def test_given_putting_stream_message_and_two_last_throws_are_not_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_not_changed_and_only_old_stream_listener_messages_are_put_in(self):
        """
        difference lays in a _old_listener_message_1 / _new_listener_message_1, new stream listener is + 1 ms
        """

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        pairs = config['instruments']['spot']

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        assert difference_depth_queue.currently_accepted_stream_id == old_stream_listener_id.id
        assert difference_depth_queue.qsize() == 3

        difference_depth_queue_content_list = [difference_depth_queue.get_nowait() for _ in
                                               range(difference_depth_queue.qsize())]

        assert (_old_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_2, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_3, mocked_timestamp_of_receive) in difference_depth_queue_content_list

        assert (_new_listener_message_1, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_2, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_3, mocked_timestamp_of_receive) not in difference_depth_queue_content_list

        DifferenceDepthQueue.clear_instances()

    @pytest.mark.skip
    def test_given_putting_stream_message_and_two_last_throws_are_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_changed_and_only_old_stream_listener_messages_are_put_in(self):

        config = {
            "instruments": {
                "spot": ["DOTUSDT", "ADAUSDT", "TRXUSDT"],
            },
            "file_duration_seconds": 30,
            "snapshot_fetcher_interval_seconds": 30,
            "websocket_life_time_seconds": 30,
            "save_to_json": True,
            "save_to_zip": False,
            "send_zip_to_blob": False
        }

        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _old_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _old_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _new_listener_message_1 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "DOTUSDT",
                    "U": 7871863945,
                    "u": 7871863947,
                    "b": [
                        [
                            "6.19800000",
                            "1816.61000000"
                        ],
                        [
                            "6.19300000",
                            "1592.79000000"
                        ]
                    ],
                    "a": [
                        [
                            "6.20800000",
                            "1910.71000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_2 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "ADAUSDT",
                    "U": 8823504433,
                    "u": 8823504452,
                    "b": [
                        [
                            "0.36440000",
                            "46561.40000000"
                        ],
                        [
                            "0.36430000",
                            "76839.90000000"
                        ],
                        [
                            "0.36400000",
                            "76688.60000000"
                        ],
                        [
                            "0.36390000",
                            "106235.50000000"
                        ],
                        [
                            "0.36370000",
                            "35413.10000000"
                        ]
                    ],
                    "a": [
                        [
                            "0.36450000",
                            "16441.60000000"
                        ],
                        [
                            "0.36460000",
                            "20497.10000000"
                        ],
                        [
                            "0.36470000",
                            "39808.80000000"
                        ],
                        [
                            "0.36480000",
                            "75106.10000000"
                        ],
                        [
                            "0.36900000",
                            "32.90000000"
                        ],
                        [
                            "0.37120000",
                            "361.70000000"
                        ]
                    ]
                }
            }
        '''

        _new_listener_message_3 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869217,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985365,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        '''

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_1)
        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_2)
        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_old_listener_message_3)
        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_1)
        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_2)
        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format(_new_listener_message_3)

        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = [difference_depth_queue.get_nowait() for _ in
                                               range(difference_depth_queue.qsize())]

        assert difference_depth_queue.currently_accepted_stream_id == new_stream_listener_id.id

        expected_list = [
            (_old_listener_message_1, mocked_timestamp_of_receive),
            (_old_listener_message_2, mocked_timestamp_of_receive),
            (_old_listener_message_3, mocked_timestamp_of_receive)
        ]

        assert difference_depth_queue_content_list == expected_list

        DifferenceDepthQueue.clear_instances()


'''
{"stream":"ethusdt@trade","data":{"e":"trade","E":1726426389417,"s":"ETHUSDT","t":1573606312,"p":"2373.76000000","q":"0.00230000","T":1726426389417,"m":false,"M":true}}
{"stream":"btcusdt@trade","data":{"e":"trade","E":1726426389463,"s":"BTCUSDT","t":3821156500,"p":"59867.99000000","q":"0.00934000","T":1726426389462,"m":true,"M":true}}
'''
