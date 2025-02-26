import queue
import time
import json
from queue import Queue
import pytest

from binance_archiver.enum_.market_enum import Market
from binance_archiver.stream_listener_id import StreamListenerId
from binance_archiver.trade_queue import TradeQueue, ClassInstancesAmountLimitException


def format_message_string_that_is_pretty_to_binance_string_format(message: str) -> str:
    message = message.strip()
    data = json.loads(message)
    compact_message = json.dumps(data, separators=(',', ':'))

    return compact_message

def add_field_to_string_json_message(message: str, field_name: str, value: any) -> str:
    return message[:-1] + f',"{field_name}":{str(value)}}}'


class TestTradeQueue:

    def test_given_pretty_printed_message_from_test_when_reformatting_then_message_is_in_binance_format(self):

        pretty_message_from_sample_test = '''            
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

        binance_format_message = format_message_string_that_is_pretty_to_binance_string_format(
            pretty_message_from_sample_test)
        assert binance_format_message == ('{"stream":"trxusdt@depth@100ms","data":{"e":"depthUpdate","E":1720337869317,'
                                          '"s":"TRXUSDT","U":4609985365,"u":4609985365,'
                                          '"b":[["0.12984000","123840.00000000"]],"a":[]}}')

    def test_given_adding_receive_timestamp_fields_to_original_binance_message_in_string_then_final_message_is_correct(self):

        pretty_message_from_sample_test = '''            
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

        binance_format_message = format_message_string_that_is_pretty_to_binance_string_format(pretty_message_from_sample_test)

        mocked_timestamp = 2115

        assert add_field_to_string_json_message(binance_format_message, "_E", mocked_timestamp) == '{"stream":"trxusdt@depth@100ms","data":{"e":"depthUpdate","E":1720337869317,"s":"TRXUSDT","U":4609985365,"u":4609985365,"b":[["0.12984000","123840.00000000"]],"a":[]},"_E":2115}'

    # TradeQueue singleton init test
    #
    def test_given_too_many_difference_depth_queue_instances_exists_when_creating_new_then_exception_is_thrown(self):
        for _ in range(3):
            TradeQueue(Market.SPOT)

        with pytest.raises(ClassInstancesAmountLimitException):
            TradeQueue(Market.SPOT)

        TradeQueue.clear_instances()

    def test_given_checking_amount_of_instances_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = TradeQueue.get_instance_count()
        assert instance_count == 0

        for _ in range(3):
            TradeQueue(Market.SPOT)

        assert TradeQueue.get_instance_count() == 3

        TradeQueue.clear_instances()

    def test_given_instances_amount_counter_reset_when_clear_instances_method_invocation_then_amount_is_zero(self):
        for _ in range(3):
            TradeQueue(Market.SPOT)

        TradeQueue.clear_instances()

        assert TradeQueue.get_instance_count() == 0
        TradeQueue.clear_instances()

    # run_mode tests
    #
    def test_given_data_listener_mode_and_global_queue_when_initializing_trade_queue_then_queue_is_set_to_global_queue(self):
        global_queue = Queue()
        trade_queue = TradeQueue(market=Market.SPOT, global_queue=global_queue)
        assert trade_queue.queue is global_queue
        TradeQueue.clear_instances()

    def test_given_trade_message_in_data_listener_mode_when_putting_message_then_message_is_added_to_global_queue(self):
        global_queue = Queue()
        trade_queue = TradeQueue(market=Market.SPOT, global_queue=global_queue)
        stream_listener_id = StreamListenerId(pairs=['BTCUSDT'])
        trade_queue.currently_accepted_stream_id_keys = stream_listener_id
        message = format_message_string_that_is_pretty_to_binance_string_format('''
        {
            "e": "trade",
            "E": 123456789,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "50000.00",
            "q": "0.1",
            "b": 88,
            "a": 50,
            "T": 123456789,
            "m": true,
            "M": true
        }
        ''')

        mocked_timestamp_of_receive = 1234567890
        trade_queue.put_trade_message(
            stream_listener_id=stream_listener_id,
            message=message,
            timestamp_of_receive=mocked_timestamp_of_receive
        )
        assert not global_queue.empty()
        queued_message = global_queue.get_nowait()
        assert queued_message == add_field_to_string_json_message(message, "_E", mocked_timestamp_of_receive)
        TradeQueue.clear_instances()

    def test_given_trade_messages_in_data_listener_mode_when_using_queue_operations_then_operations_reflect_global_queue_state(self):
        global_queue = Queue()
        trade_queue = TradeQueue(market=Market.SPOT, global_queue=global_queue)
        stream_listener_id = StreamListenerId(pairs=['ETHUSDT'])
        trade_queue.currently_accepted_stream_id_keys = stream_listener_id
        message = format_message_string_that_is_pretty_to_binance_string_format('''
        {
            "e": "trade",
            "E": 123456789,
            "s": "ETHUSDT",
            "t": 12346,
            "p": "4000.00",
            "q": "0.5",
            "b": 89,
            "a": 51,
            "T": 123456789,
            "m": false,
            "M": true
        }
        ''')
        timestamp_of_receive = 1234567890
        trade_queue.put_trade_message(stream_listener_id, message, timestamp_of_receive)
        assert trade_queue.qsize() == 1
        assert not trade_queue.empty()
        queued_message = trade_queue.get_nowait()
        assert queued_message == add_field_to_string_json_message(message, "_E", timestamp_of_receive)
        assert trade_queue.empty()
        TradeQueue.clear_instances()

    def test_given_empty_global_queue_in_data_listener_mode_when_checking_queue_then_empty_and_get_nowait_raises_exception(self):
        global_queue = Queue()
        tq = TradeQueue(market=Market.SPOT, global_queue=global_queue)
        assert tq.empty()
        with pytest.raises(queue.Empty):
            tq.get_nowait()
        TradeQueue.clear_instances()

    # _put_with_no_repetitions test
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

        pairs = config['instruments']['spot']

        trade_queue: TradeQueue = TradeQueue(market=Market.SPOT)

        first_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)

        trade_queue.currently_accepted_stream_id_keys = first_stream_listener_id
        mocked_timestamp_of_receive = 2115

        _first_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format(
            '''
            {
              "stream": "adausdt@trade",
              "data": {
                "e": "trade",
                "E": 1726688099009,
                "s": "ADAUSDT",
                "t": 506142454,
                "p": "0.33570000",
                "q": "15.70000000",
                "T": 1726688099008,
                "m": false,
                "M": true
              }
            }            
            '''
        )

        trade_queue.put_trade_message(
            stream_listener_id=first_stream_listener_id,
            message=_first_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue_content_list = []

        while trade_queue.qsize() > 0:
            trade_queue_content_list.append(trade_queue.get_nowait())
        assert (add_field_to_string_json_message(_first_listener_message_1, "_E", mocked_timestamp_of_receive)
                in trade_queue_content_list)
        assert len(trade_queue_content_list) == 1

        TradeQueue.clear_instances()

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )


        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=2115
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        trade_queue_content_list = []

        while trade_queue.qsize() > 0:
            trade_queue_content_list.append(trade_queue.get_nowait())

        expected_trade_queue_content_list = [
            add_field_to_string_json_message(_old_listener_message_1, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_old_listener_message_2, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_old_listener_message_3, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_new_listener_message_4, "_E", mocked_timestamp_of_receive)
        ]

        assert trade_queue_content_list == expected_trade_queue_content_list

        TradeQueue.clear_instances()

    #get_message_signs
    #
    def test_given_getting_message_signs_whilst_putting_message_when_get_message_signs_then_signs_are_returned_correctly(self):
        message = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        assert TradeQueue.get_message_signs(message) == '"s":"ADAUSDT","t":506142454'
        TradeQueue.clear_instances()

    # get, get_nowait, clear, empty, qsize
    #
    def test_given_getting_messages_when_get_nowait_then_qsize_is_zero_and_got_list_is_ok(self):

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=2115
        )


        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=2115
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=2115
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        trade_queue_content_list = []

        while trade_queue.qsize() > 0:
            trade_queue_content_list.append(trade_queue.get_nowait())

        assert trade_queue.qsize() == 0

        expected_trade_queue_content_list = [
            add_field_to_string_json_message(_old_listener_message_1, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_old_listener_message_2, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_old_listener_message_3, "_E", mocked_timestamp_of_receive),
            add_field_to_string_json_message(_new_listener_message_4, "_E", mocked_timestamp_of_receive)
        ]

        assert trade_queue_content_list == expected_trade_queue_content_list

        TradeQueue.clear_instances()

    def test_given_getting_from_queue_when_get_nowait_then_last_element_is_returned(self):

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        presumed_first_transaction = trade_queue.get_nowait()

        assert presumed_first_transaction == add_field_to_string_json_message(_old_listener_message_1, "_E", mocked_timestamp_of_receive)

        TradeQueue.clear_instances()

    def test_getting_with_no_wait_from_queue_when_method_invocation_then_last_element_is_returned(self):

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        presumed_first_transaction = trade_queue.get()

        assert presumed_first_transaction == add_field_to_string_json_message(_old_listener_message_1, "_E", mocked_timestamp_of_receive)

        TradeQueue.clear_instances()

    def test_given_clearing_difference_depth_queue_when_invocation_then_qsize_equals_zero(self):

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        trade_queue.clear()

        assert trade_queue.qsize() == 0

        TradeQueue.clear_instances()

    def test_given_checking_empty_when_method_invocation_then_result_is_ok(self):

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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4
        assert trade_queue.empty() == False

        trade_queue.clear()

        assert trade_queue.qsize() == 0
        assert trade_queue.empty() == True

        TradeQueue.clear_instances()

    def test_checking_size_when_method_invocation_then_result_is_ok(self):
        """change on _new_listener_message_3"""
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

        trade_queue = TradeQueue(market=Market.SPOT)

        pairs = config['instruments']['spot']

        old_stream_listener_id = StreamListenerId(pairs=pairs)
        time.sleep(0.01)
        new_stream_listener_id = StreamListenerId(pairs=pairs)

        assert old_stream_listener_id.pairs_amount == 3
        assert new_stream_listener_id.pairs_amount == 3

        mocked_timestamp_of_receive = 2115

        trade_queue.currently_accepted_stream_id_keys = old_stream_listener_id

        _old_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_1 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142454,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_2 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "adausdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688099009,
            "s": "ADAUSDT",
            "t": 506142455,
            "p": "0.33570000",
            "q": "15.70000000",
            "T": 1726688099008,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_3 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "dotusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100060,
            "s": "DOTUSDT",
            "t": 366582616,
            "p": "4.06600000",
            "q": "106.87000000",
            "T": 1726688100060,
            "m": false,
            "M": true
          }
        }
        ''')

        _old_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''            
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100727,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        _new_listener_message_4 = format_message_string_that_is_pretty_to_binance_string_format('''
        {
          "stream": "trxusdt@trade",
          "data": {
            "e": "trade",
            "E": 1726688100728,
            "s": "TRXUSDT",
            "t": 295482474,
            "p": "0.14920000",
            "q": "128.00000000",
            "T": 1726688100727,
            "m": false,
            "M": true
          }
        }
        ''')

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_1,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_2,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_3,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=old_stream_listener_id,
            message=_old_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        trade_queue.put_trade_message(
            stream_listener_id=new_stream_listener_id,
            message=_new_listener_message_4,
            timestamp_of_receive=mocked_timestamp_of_receive
        )

        assert trade_queue.currently_accepted_stream_id_keys == new_stream_listener_id
        assert trade_queue.qsize() == 4

        trade_queue.clear()

        assert trade_queue.qsize() == 0

        TradeQueue.clear_instances()
