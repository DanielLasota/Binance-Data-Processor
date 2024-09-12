import time
import pytest

from binance_archiver.orderbook_level_2_listener.difference_depth_queue import DifferenceDepthQueue, \
    ClassInstancesAmountLimitException
from binance_archiver.orderbook_level_2_listener.market_enum import Market
from binance_archiver.orderbook_level_2_listener.stream_id import StreamId


class TestDifferenceDepthQueue:

    def test_given_too_many_difference_depth_queue_instances_exists_when_creating_new_then_exception_is_thrown(self):
        for _ in range(4):
            DifferenceDepthQueue(Market.SPOT)

        with pytest.raises(ClassInstancesAmountLimitException):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

    #

    def test_given_checking_amount_of_instances_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = DifferenceDepthQueue.get_instance_count()
        assert instance_count == 0

        for _ in range(4):
            DifferenceDepthQueue(Market.SPOT)

        assert DifferenceDepthQueue.get_instance_count() == 4

        DifferenceDepthQueue.clear_instances()

    def test_given_instances_amount_counter_reset_when_clear_instances_method_invocation_then_amount_is_zero(self):
        for _ in range(4):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

        assert DifferenceDepthQueue.get_instance_count() == 0
        DifferenceDepthQueue.clear_instances()

    #

    def test_given_putting_message_from_no_longer_accepted_stream_listener_id_when_try_to_put_then_message_is_not_added_to_queue(self):

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
        DifferenceDepthQueue.clear_instances()

    #

    def test_given_putting_message_when_adding_two_different_stream_listeners_message_throws_to_compare_structure_then_structure_is_ok(self):
        """throws are different and the difference lays in adausdt _new_listener_message_1"""
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

        import json
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

        expected_comparison_structure = {
            old_stream_listener_id.id: [
                json.loads(_old_listener_message_1),
                json.loads(_old_listener_message_2),
                json.loads(_old_listener_message_3)
            ],
            new_stream_listener_id.id: [
                json.loads(_new_listener_message_1),
                json.loads(_new_listener_message_2)
            ]
        }

        assert difference_depth_queue._two_last_throws == expected_comparison_structure

        assert old_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert new_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert len(difference_depth_queue._two_last_throws[old_stream_listener_id.id]) == 3
        assert len(difference_depth_queue._two_last_throws[new_stream_listener_id.id]) == 2

        DifferenceDepthQueue.clear_instances()

    def test_given_putting_message_when_next_100_ms_throws_is_being_compared_then_structure_is_properly_cleared_with_only_new_message_in_dict(self):
        """throws are different and the difference lays in adausdt _new_listener_message_1"""
        import json

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

        _new_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869315,
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
                    "E": 1720337869316,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        expected_comparison_structure = {
            new_stream_listener_id.id: [
                json.loads(_new_listener_message_4)
            ]
        }

        assert difference_depth_queue._two_last_throws == expected_comparison_structure

        assert old_stream_listener_id.id not in difference_depth_queue._two_last_throws
        assert new_stream_listener_id.id in difference_depth_queue._two_last_throws
        assert len(difference_depth_queue._two_last_throws[new_stream_listener_id.id]) == 1

        DifferenceDepthQueue.clear_instances()

    def test_given_huge_amount_of_stream_listener_changes_when_comparing_message_queues_there_is_at_max_two_compare_structure_dict_keys(self):
        """throws are set to be equal each after other"""
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == third_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        DifferenceDepthQueue.clear_instances()

    #

    def test_given_putting_message_when_putting_message_of_currently_accepted_stream_id_then_message_is_being_added_to_the_queue(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)

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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert (_first_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert len(difference_depth_queue_content_list) == 1

        DifferenceDepthQueue.clear_instances()


    #

    def test_given_putting_stream_message_and_two_last_throws_are_not_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_not_changed_and_only_old_stream_listener_messages_are_put_in(self):
        '''
        difference lays in a message 1 dotusdt bid entry, new stream listener is +1 ms
        '''

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

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert (_old_listener_message_1, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_2, mocked_timestamp_of_receive) in difference_depth_queue_content_list
        assert (_old_listener_message_3, mocked_timestamp_of_receive) in difference_depth_queue_content_list

        assert (_new_listener_message_1, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_2, mocked_timestamp_of_receive) not in difference_depth_queue_content_list
        assert (_new_listener_message_3, mocked_timestamp_of_receive) not in difference_depth_queue_content_list

        DifferenceDepthQueue.clear_instances()

    def test_given_putting_stream_message_and_two_last_throws_are_equal_when_two_listeners_messages_are_being_compared_then_currently_accepted_stream_id_is_changed_and_only_old_stream_listener_messages_are_put_in(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        difference_depth_queue_content_list = []
        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert difference_depth_queue.currently_accepted_stream_id == new_stream_listener_id.id

        expected_list = [
            (_old_listener_message_1, mocked_timestamp_of_receive),
            (_old_listener_message_2, mocked_timestamp_of_receive),
            (_old_listener_message_3, mocked_timestamp_of_receive)
        ]

        assert difference_depth_queue_content_list == expected_list

        DifferenceDepthQueue.clear_instances()


    #

    def test_given_putting_messages_whilst_changing_and_whilst_usual_working_mode_when_get_nowait_from_queue_then_order_and_amount_of_queue_elements_is_ok(self):
        """throws are set to be equal each after other"""
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        expected_list = [
            (_first_listener_message_1, mocked_timestamp_of_receive),
            (_first_listener_message_2, mocked_timestamp_of_receive),
            (_first_listener_message_3, mocked_timestamp_of_receive),
            (_second_listener_message_4, mocked_timestamp_of_receive),
            (_second_listener_message_5, mocked_timestamp_of_receive),
            (_second_listener_message_6, mocked_timestamp_of_receive),
        ]

        assert difference_depth_queue_content_list == expected_list

        DifferenceDepthQueue.clear_instances()

    #

    def test_given_comparing_two_throws_when_throws_are_equal_then_method_returns_true(self):

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

        _two_last_throws = {}

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _two_last_throws[old_stream_listener_id.id] = []
        _two_last_throws[new_stream_listener_id.id] = []

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(old_stream_listener_id.pairs_amount, _two_last_throws)
        assert do_they_match is True
        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(new_stream_listener_id.pairs_amount, _two_last_throws)
        assert do_they_match is True
        DifferenceDepthQueue.clear_instances()

    def test_given_comparing_two_throws_when_throws_are_not_equal_then_method_returns_false(self):
        """the throws are not equal and the difference lays in trxusdt"""

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

        _two_last_throws = {}

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _two_last_throws[old_stream_listener_id.id] = []
        _two_last_throws[new_stream_listener_id.id] = []

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869216,
                    "s": "TRXUSDT",
                    "U": 4609985365,
                    "u": 4609985366,
                    "b": [
                        [
                            "0.12984000",
                            "123840.00000000"
                        ],
                        [
                            "0.10000000",
                            "123841.00000000"
                        ]
                    ],
                    "a": [

                    ]
                }
            }
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        do_they_match = DifferenceDepthQueue._do_last_two_throws_match(3, _two_last_throws)

        assert do_they_match is False
        DifferenceDepthQueue.clear_instances()

    def test_given_comparing_two_throws_when_sorting_messages_then_dict_is_sorted(self):

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

        _two_last_throws = {}

        old_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamId(pairs=pairs)

        _two_last_throws[old_stream_listener_id.id] = []
        _two_last_throws[new_stream_listener_id.id] = []

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[new_stream_listener_id.id].append(
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        _two_last_throws[old_stream_listener_id.id].append(
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869215,
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
        )

        sorted_dict = DifferenceDepthQueue._sort_entries_by_symbol(_two_last_throws)

        assert sorted_dict[new_stream_listener_id.id][0]['stream'] == 'adausdt@depth@100ms'
        assert sorted_dict[new_stream_listener_id.id][1]['stream'] == 'dotusdt@depth@100ms'
        assert sorted_dict[new_stream_listener_id.id][2]['stream'] == 'trxusdt@depth@100ms'

        assert sorted_dict[old_stream_listener_id.id][0]['stream'] == 'adausdt@depth@100ms'
        assert sorted_dict[old_stream_listener_id.id][1]['stream'] == 'dotusdt@depth@100ms'
        assert sorted_dict[old_stream_listener_id.id][2]['stream'] == 'trxusdt@depth@100ms'
        DifferenceDepthQueue.clear_instances()

    #

    def test_getting_from_queue_when_method_invocation_then_last_element_is_returned(self):
        """throws are set to be equal to cause change each after other"""

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == third_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get())

        assert difference_depth_queue_content_list[0] == (_first_listener_message_1, mocked_timestamp_of_receive)
        DifferenceDepthQueue.clear_instances()

    def test_getting_with_no_wait_from_queue_when_method_invocation_then_last_element_is_returned(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == third_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get_nowait())

        assert difference_depth_queue_content_list[0] == (_first_listener_message_1, mocked_timestamp_of_receive)
        DifferenceDepthQueue.clear_instances()

    #

    def test_given_clearing_difference_depth_queue_when_invocation_then_qsize_equals_zero(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.clear()

        assert difference_depth_queue.qsize() == 0
        DifferenceDepthQueue.clear_instances()

    def test_given_checking_empty_when_method_invocation_then_result_is_ok(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.empty() is False

        difference_depth_queue_content_list = []

        while difference_depth_queue.qsize() > 0:
            difference_depth_queue_content_list.append(difference_depth_queue.get())

        difference_depth_queue.clear()

        assert difference_depth_queue.qsize() == 0
        assert difference_depth_queue.empty() is True

        DifferenceDepthQueue.clear_instances()

    def test_checking_size_when_method_invocation_then_result_is_ok(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

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

        first_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        second_stream_listener_id = StreamId(pairs=pairs)
        time.sleep(0.01)
        third_stream_listener_id = StreamId(pairs=pairs)

        mocked_timestamp_of_receive = 2115

        difference_depth_queue.currently_accepted_stream_id = first_stream_listener_id.id

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

        _first_listener_message_2 = '''            
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

        _first_listener_message_3 = '''            
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

        _second_listener_message_1 = '''            
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

        _second_listener_message_2 = '''            
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

        _second_listener_message_3 = '''            
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.currently_accepted_stream_id == second_stream_listener_id.id
        assert difference_depth_queue._two_last_throws == {}

        _second_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _second_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_4 = '''            
            {
                "stream": "dotusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_5 = '''            
            {
                "stream": "adausdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        _third_listener_message_6 = '''            
            {
                "stream": "trxusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1720337869317,
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

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=second_stream_listener_id,
            message=_second_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_5,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        difference_depth_queue.put_queue_message(
            stream_listener_id=third_stream_listener_id,
            message=_third_listener_message_6,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert difference_depth_queue.qsize() == 6
        difference_depth_queue.clear_instances()
