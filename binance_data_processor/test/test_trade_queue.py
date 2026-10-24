import queue
import json
from queue import Queue
import pytest

from binance_data_processor.enums.market_enum import Market
from binance_data_processor.core.trade_queue import TradeQueue, ClassInstancesAmountLimitException, SwitchingWsStrategy


def format_pretty_msg_to_binance_str_format(msg: str) -> str:
    msg = msg.strip()
    data = json.loads(msg)
    compact_message = json.dumps(data, separators=(',', ':'))

    return compact_message

def add_field_to_json(msg: str, field_name: str, val: any) -> str:
    return msg[:-1] + f',"{field_name}":{str(val)}}}'


class TestTradeQueue:

    def test_given_pretty_msg_when_formatting_then_msg_is_binance_format(self):

        pretty_test_msg = '''            
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

        binance_format_msg = format_pretty_msg_to_binance_str_format(
            pretty_test_msg)
        assert binance_format_msg == (
            '{"stream":"trxusdt@depth@100ms","data":{"e":"depthUpdate","E":1720337869317,"s":"TRXUSDT","U":4609985365,'
            '"u":4609985365,"b":[["0.12984000","123840.00000000"]],"a":[]}}'
        )

    def test_given_adding_receive_ts_fields_to_binance_msg_then_final_message_is_correct(self):

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

        binance_format_message = format_pretty_msg_to_binance_str_format(pretty_message_from_sample_test)

        mocked_timestamp = 2115

        assert add_field_to_json(binance_format_message, "_E", mocked_timestamp) == '{"stream":"trxusdt@depth@100ms","data":{"e":"depthUpdate","E":1720337869317,"s":"TRXUSDT","U":4609985365,"u":4609985365,"b":[["0.12984000","123840.00000000"]],"a":[]},"_E":2115}'

    # TradeQueue singleton init test
    #
    def test_given_too_many_difference_depth_queue_instances_exists_when_creating_new_then_exception_is_thrown(self):
        x = TradeQueue(Market.SPOT, pairs_n=3)
        y = TradeQueue(Market.USD_M_FUTURES, pairs_n=3)
        z = TradeQueue(Market.COIN_M_FUTURES, pairs_n=3)

        with pytest.raises(ClassInstancesAmountLimitException):
            TradeQueue(Market.SPOT, pairs_n=3)

        for _ in [x, y, z]: _.close()
        TradeQueue.clear_instances()

    def test_given_checking_amount_of_instances_when_get_instance_count_invocation_then_amount_is_correct(self):
        instance_count = TradeQueue.get_instance_count()
        assert instance_count == 0

        x = TradeQueue(Market.SPOT, pairs_n=3)
        y = TradeQueue(Market.USD_M_FUTURES, pairs_n=3)
        z = TradeQueue(Market.COIN_M_FUTURES, pairs_n=3)

        assert TradeQueue.get_instance_count() == 3

        for _ in [x, y, z]: _.close()
        TradeQueue.clear_instances()

    def test_given_instances_amount_counter_reset_when_clear_instances_method_invocation_then_amount_is_zero(self):
        x = TradeQueue(Market.SPOT, pairs_n=3)
        y = TradeQueue(Market.USD_M_FUTURES, pairs_n=3)
        z = TradeQueue(Market.COIN_M_FUTURES, pairs_n=3)

        TradeQueue.clear_instances()

        assert TradeQueue.get_instance_count() == 0
        for _ in [x, y, z]: _.close()
        TradeQueue.clear_instances()

    # run_mode tests
    #
    def test_given_data_listener_mode_and_global_queue_when_initializing_trade_queue_then_queue_is_set_to_global_queue(self):
        global_queue = Queue()
        tq = TradeQueue(market=Market.SPOT, global_queue=global_queue, pairs_n=3)
        assert tq.queue is global_queue

        TradeQueue.clear_instances()

    def test_given_trade_message_in_data_listener_mode_when_putting_message_then_message_is_added_to_global_queue(self):
        global_queue = Queue()
        tq = TradeQueue(Market.SPOT, pairs_n=3, global_queue=global_queue)
        client_id = False
        tq.accepted_cid = client_id

        msg = format_pretty_msg_to_binance_str_format('''
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

        test_ts = 1234567890
        tq.put(cid=client_id, msg=msg, ts=test_ts)

        assert not global_queue.empty()
        msg_from_q = global_queue.get_nowait()
        assert msg_from_q == add_field_to_json(msg, "_E", test_ts)

        TradeQueue.clear_instances()

    def test_given_trade_messages_in_data_listener_mode_when_using_queue_operations_then_operations_reflect_global_queue_state(self):
        global_queue = Queue()
        tq = TradeQueue(market=Market.SPOT, pairs_n=3, global_queue=global_queue)
        client_id = False
        tq.accepted_cid = client_id
        msg = format_pretty_msg_to_binance_str_format('''
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
        tq.put(client_id, msg, timestamp_of_receive)

        assert tq.qsize() == 1
        assert not tq.empty()
        queued_message = tq.get_nowait()
        assert queued_message == add_field_to_json(msg, "_E", timestamp_of_receive)
        assert tq.empty()

        TradeQueue.clear_instances()

    def test_given_empty_global_queue_in_data_listener_mode_when_checking_queue_then_empty_and_get_nowait_raises_exception(self):
        global_queue = Queue()
        tq = TradeQueue(market=Market.SPOT, pairs_n=3, global_queue=global_queue)
        assert tq.empty()
        with pytest.raises(queue.Empty):
            tq.get_nowait()

        TradeQueue.clear_instances()

    # _put_with_no_repetitions test
    #

    def test_given_putting_message_when_putting_message_of_currently_accepted_stream_id_then_message_is_being_added_to_the_queue(self):
        cfg = {
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

        tq = TradeQueue(market=Market.SPOT, pairs_n=3)

        client_id = False

        tq.accepted_cid = client_id
        test_ts = 2115

        _msg1 = format_pretty_msg_to_binance_str_format(
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

        tq.put(
            cid=client_id,
            msg=_msg1,
            ts=test_ts
        )

        tq_content = []

        while tq.qsize() > 0:
            tq_content.append(tq.get_nowait())
        assert (add_field_to_json(_msg1, "_E", test_ts) in tq_content)
        assert len(tq_content) == 1

        tq.close()
        TradeQueue.clear_instances()

    def test_given_put_msg_from_no_longer_accepted_client_id_when_put_then_msg_is_not_added_to_the_queue(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_client_id = False
        new_client_id = True

        test_ts = 2115

        tq.accepted_cid = old_client_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=2115)
        tq.put(cid=old_client_id, msg=_old_listener_message_2, ts=2115)
        tq.put(cid=new_client_id, msg=_new_listener_message_1, ts=2115)
        tq.put(cid=new_client_id, msg=_new_listener_message_2, ts=2115)
        tq.put(cid=old_client_id, msg=_old_listener_message_3, ts=2115)
        tq.put(cid=new_client_id, msg=_new_listener_message_3, ts=2115)
        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=2115)
        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=2115)

        assert tq.accepted_cid == new_client_id

        tq_content = []

        while tq.qsize() > 0:
            tq_content.append(tq.get_nowait())

        expected_tq_content = [
            add_field_to_json(_old_listener_message_1, "_E", test_ts),
            add_field_to_json(_old_listener_message_2, "_E", test_ts),
            add_field_to_json(_old_listener_message_3, "_E", test_ts),
            add_field_to_json(_new_listener_message_4, "_E", test_ts)
        ]

        assert tq_content == expected_tq_content

        tq.close()
        TradeQueue.clear_instances()

    #get_message_signs
    #
    def test_given_getting_message_signs_whilst_putting_message_when_get_message_signs_then_signs_are_returned_correctly(self):
        msg = format_pretty_msg_to_binance_str_format('''            
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

        assert SwitchingWsStrategy.get_msg_signs(msg) == '"s":"ADAUSDT","t":506142454'

    # get, get_nowait, clear, empty, qsize
    #
    def test_given_getting_messages_when_get_nowait_then_qsize_is_zero_and_got_list_is_ok(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_stream_listener_id = False
        new_stream_listener_id = True

        test_ts = 2115

        tq.accepted_cid = old_stream_listener_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_1, ts=2115)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_2, ts=2115)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_1, ts=2115)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_2, ts=2115)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_3, ts=2115)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_3, ts=2115)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_4, ts=2115)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_4, ts=2115)

        assert tq.accepted_cid == new_stream_listener_id
        # assert tq.qsize() == 4

        trade_queue_content_list = []

        while tq.qsize() > 0:
            trade_queue_content_list.append(tq.get_nowait())

        assert tq.qsize() == 0

        expected_tq = [
            add_field_to_json(_old_listener_message_1, "_E", test_ts),
            add_field_to_json(_old_listener_message_2, "_E", test_ts),
            add_field_to_json(_old_listener_message_3, "_E", test_ts),
            add_field_to_json(_new_listener_message_4, "_E", test_ts)
        ]

        assert trade_queue_content_list == expected_tq

        tq.close()
        TradeQueue.clear_instances()

    def test_given_getting_from_queue_when_get_nowait_then_last_element_is_returned(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_client_id = False
        new_client_id = True

        test_ts = 2115

        tq.accepted_cid = old_client_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_2, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_1, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_2, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_3, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_3, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=test_ts)

        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=test_ts)

        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=test_ts)

        assert tq.accepted_cid == new_client_id

        expected_first_trade = tq.get_nowait()

        assert expected_first_trade == add_field_to_json(_old_listener_message_1, "_E", test_ts)

        tq.close()
        TradeQueue.clear_instances()

    def test_getting_with_no_wait_from_queue_when_method_invocation_then_last_element_is_returned(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_client_id = False
        new_client_id = True

        test_ts = 2115

        tq.accepted_cid = old_client_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=test_ts)

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_2, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_1, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_2, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_3, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_3, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=test_ts)

        assert tq.accepted_cid == new_client_id

        expected_first_trade = tq.get()

        assert expected_first_trade == add_field_to_json(_old_listener_message_1, "_E", test_ts)

        tq.close()
        TradeQueue.clear_instances()

    def test_given_clearing_difference_depth_queue_when_invocation_then_qsize_equals_zero(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_stream_listener_id = False
        new_stream_listener_id = True

        mocked_timestamp_of_receive = 2115

        tq.accepted_cid = old_stream_listener_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_1, ts=mocked_timestamp_of_receive)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_2, ts=mocked_timestamp_of_receive)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_1, ts=mocked_timestamp_of_receive)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_2, ts=mocked_timestamp_of_receive)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_3, ts=mocked_timestamp_of_receive)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_3, ts=mocked_timestamp_of_receive)
        tq.put(cid=old_stream_listener_id, msg=_old_listener_message_4, ts=mocked_timestamp_of_receive)
        tq.put(cid=new_stream_listener_id, msg=_new_listener_message_4, ts=mocked_timestamp_of_receive)

        assert tq.accepted_cid == new_stream_listener_id

        tq.clear()
        assert tq.qsize() == 0

        tq.close()
        TradeQueue.clear_instances()

    def test_given_checking_empty_when_method_invocation_then_result_is_ok(self):

        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_client_id = False
        new_client_id = True

        test_ts = 2115

        tq.accepted_cid = old_client_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_2, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_1, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_2, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_3, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_3, ts=test_ts)
        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=test_ts)
        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=test_ts)

        assert tq.accepted_cid == new_client_id
        assert not tq.empty()

        tq.clear()

        assert tq.qsize() == 0
        assert tq.empty() == True

        tq.close()
        TradeQueue.clear_instances()

    def test_checking_size_when_method_invocation_then_result_is_ok(self):
        """change on _new_listener_message_3"""
        cfg = {
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

        tq = TradeQueue(Market.SPOT, 3)
        tq.set_switching_ws_mode()

        old_client_id = False
        new_client_id = True

        test_tq = 2115

        tq.accepted_cid = old_client_id

        _old_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_1 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_2 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_3 = format_pretty_msg_to_binance_str_format('''            
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

        _old_listener_message_4 = format_pretty_msg_to_binance_str_format('''            
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

        _new_listener_message_4 = format_pretty_msg_to_binance_str_format('''
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

        tq.put(cid=old_client_id, msg=_old_listener_message_1, ts=test_tq)
        tq.put(cid=old_client_id, msg=_old_listener_message_2, ts=test_tq)
        tq.put(cid=new_client_id, msg=_new_listener_message_1, ts=test_tq)
        tq.put(cid=new_client_id, msg=_new_listener_message_2, ts=test_tq)
        tq.put(cid=old_client_id, msg=_old_listener_message_3, ts=test_tq)
        tq.put(cid=new_client_id, msg=_new_listener_message_3, ts=test_tq)
        tq.put(cid=old_client_id, msg=_old_listener_message_4, ts=test_tq)
        tq.put(cid=new_client_id, msg=_new_listener_message_4, ts=test_tq)

        assert tq.accepted_cid == new_client_id
        assert tq.qsize() > 0
        assert not tq.empty()

        tq.clear()

        assert tq.qsize() == 0

        tq.close()
        TradeQueue.clear_instances()
