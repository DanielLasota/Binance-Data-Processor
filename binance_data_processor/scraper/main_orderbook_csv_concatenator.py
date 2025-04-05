from orderbook_csv_concatenator import make_concatenated_csvs


if __name__ == '__main__':

    make_concatenated_csvs(
        date_range=['01-04-2025', '01-04-2025'],
        pairs=[
            'TRXUSDT',
            'ADAUSDT'
        ],
        markets=[
            'SPOT',
            'USD_M_FUTURES',
            'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            # 'DEPTH_SNAPSHOT',
        ],
        should_join_pairs_into_one_csv=False,
        should_join_markets_into_one_csv=True,
        csvs_nest_catalog='C:/Users/daniel/Documents/binance_archival_data/',
        dump_catalog='C:/Users/daniel/Documents/sample_merged_csvs_for_ob/'
    )
