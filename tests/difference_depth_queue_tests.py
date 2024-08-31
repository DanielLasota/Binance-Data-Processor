import time

import pytest

from binance_archiver.orderbook_level_2_listener.difference_depth_queue import DifferenceDepthQueue, \
    ClassInstancesAmountLimitException
from binance_archiver.orderbook_level_2_listener.market_enum import Market

from binance_archiver.orderbook_level_2_listener.stream_id import StreamId


class TestDifferenceDepthQueue:

    def test_given_too_many_difference_depth_queue_instances_then_exception_is_thrown(self):
        for _ in range(6):
            DifferenceDepthQueue(Market.SPOT)

        with pytest.raises(ClassInstancesAmountLimitException):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

    def test_given_difference_depth_queues_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = DifferenceDepthQueue.get_instance_count()
        assert instance_count == 0

        for _ in range(6):
            DifferenceDepthQueue(Market.SPOT)

        assert DifferenceDepthQueue.get_instance_count() == 6

        DifferenceDepthQueue.clear_instances()

    def test_given_difference_depth_queues_when_clear_instances_invocation_then_amount_is_zero(self):
        for _ in range(6):
            DifferenceDepthQueue(Market.SPOT)

        DifferenceDepthQueue.clear_instances()

        assert DifferenceDepthQueue.get_instance_count() == 0

    @pytest.mark.skip
    def test_given__do_they_match_when_comparing_two_different_stream_listeners_throws_then_currently_accepted_stream_id_is_not_changed(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId()
        time.sleep(0.01)
        new_stream_listener_id = StreamId()

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
        )
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
        )
        '''


        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=0
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=0
        )
        difference_depth_queue.put_queue_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=0
        )


        _new_listener_message_1 = '''            
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

        _new_listener_message_2 = '''            
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
        '''

        _new_listener_message_3 = '''            
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
        '''


        assert difference_depth_queue.qsize() == 1

    def test_given__do_they_match_when_comparing_two_equal_stream_listeners_throws_then_currently_accepted_stream_id_is_changed(self):
        ...

    def test_given_stream_listener_are_equal_when_set_new_stream_id_as_currently_accepted_then_new_stream_is_set_properly(self):
        ...

    def test_given_two_equal_throws_when_compare_then_method_returns_true(self):
        _two_last_throws = {}

        old_stream_listener_id = StreamId()
        time.sleep(0.01)
        new_stream_listener_id = StreamId()

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

        do_they_match = DifferenceDepthQueue._compare_two_last_throws(3, _two_last_throws)

        assert do_they_match is True

    def test_given_two_different_throws_when_compare_then_method_returns_false(self):
        _two_last_throws = {}

        old_stream_listener_id = StreamId()
        time.sleep(0.01)
        new_stream_listener_id = StreamId()

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
        # old trx entry is different from new trx entry

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
        # old trx entry is different from new trx entry

        do_they_match = DifferenceDepthQueue._compare_two_last_throws(3, _two_last_throws)

        assert do_they_match is False

    def test_given_two_throws_does__sort_entries_by_symbol_sort_entries_by_symbol(self):
        _two_last_throws = {}

        old_stream_listener_id = StreamId()
        time.sleep(0.01)
        new_stream_listener_id = StreamId()

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

    def test_given_put_queue_message_when_only_one_stream_listener_is_working_then_entry_is_being_added(self):
        ...

    def test_given_put_queue_message_when_two_stream_listeners_are_working_and_old_stream_listener_entry_should_be_added_then_old_entry_is_being_added(self):
        ...

    def test_given_put_queue_message_when_two_stream_listeners_are_working_and_new_stream_listener_entry_should_be_added_then_new_entry_is_being_added(self):
        ...

    def test_given_put_queue_message_when_two_stream_listeners_are_working_and_entries_are_equal_and_instrument_number_is_appropriate_then_stream_listener_ids_are_being_changed(self):
        ...

    def test_given_difference_queue_depth_when_get_method_is_called_then_last_entry_is_being_returned(self):
        ...

    def test_given_difference_queue_depth_when_get_no_wait_method_is_called_then_last_entry_is_being_returned(self):
        ...

    def test_given_difference_queue_depth_when_clear_then_qsize_equals_zero(self):
        ...

    def test_given_difference_queue_depth_when_empty_method_is_called_then_result_is_appropriate(self):
        ...

    def test_given_difference_queue_depth_when_qsize_method_call_then_qsize_is_ok(self):
        ...













    @pytest.mark.skip
    def test_given_two_equal_throws_does_method_return_true(self):
        difference_depth_queue = DifferenceDepthQueue(market=Market.SPOT)

        old_stream_listener_id = StreamId()
        time.sleep(0.01)
        new_stream_listener_id = StreamId()

        difference_depth_queue.currently_accepted_stream_id = old_stream_listener_id.id

        _message='''            
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
            stream_listener_id=old_stream_listener_id,
            message=_message,
            timestamp_of_receive=0
        )

        assert difference_depth_queue.qsize() == 1