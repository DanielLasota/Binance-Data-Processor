import pprint

from binance_archiver.orderbook_level_2_listener.SupervisedQueue import SupervisedQueue
from binance_archiver.orderbook_level_2_listener.stream_age_enum import StreamAge

import re
from datetime import datetime, timezone


class TestSupervisedQueue:

    def test_given_two_equal_throws_does_method_return_true(self):
        _two_last_throws = {
            StreamAge.NEW: [],
            StreamAge.OLD: []
        }

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        do_they_match = SupervisedQueue._compare_two_last_throws(3, _two_last_throws)

        assert do_they_match is True

    def test_given_two_different_throws_does_method_return_false(self):
        _two_last_throws = {
            StreamAge.NEW: [],
            StreamAge.OLD: []
        }

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        do_they_match = SupervisedQueue._compare_two_last_throws(3, _two_last_throws)

        assert do_they_match is False

    def test_given_two_throws_does_method_sort_entries_by_symbol(self):
        _two_last_throws = {
            StreamAge.NEW: [],
            StreamAge.OLD: []
        }

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.NEW].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        _two_last_throws[StreamAge.OLD].append(
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

        sorted_dict = SupervisedQueue.sort_entries_by_symbol(_two_last_throws)

        assert sorted_dict[StreamAge.NEW][0]['stream'] == 'adausdt@depth@100ms'
        assert sorted_dict[StreamAge.NEW][1]['stream'] == 'dotusdt@depth@100ms'
        assert sorted_dict[StreamAge.NEW][2]['stream'] == 'trxusdt@depth@100ms'

        assert sorted_dict[StreamAge.OLD][0]['stream'] == 'adausdt@depth@100ms'
        assert sorted_dict[StreamAge.OLD][1]['stream'] == 'dotusdt@depth@100ms'
        assert sorted_dict[StreamAge.OLD][2]['stream'] == 'trxusdt@depth@100ms'