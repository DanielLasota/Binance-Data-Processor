from binance_data_processor import make_merged_csvs


if __name__ == '__main__':
    # dane gituwa od 28.05.2025 wlacznie

    make_merged_csvs(
        date_range=['02-07-2025', '02-07-2025'],
        pairs=[
            # "BTCUSDT",
            "SOLUSDT",
            # "XRPUSDT",
        ],
        markets=[
            # 'SPOT',
            'USD_M_FUTURES',
            # 'COIN_M_FUTURES'
        ],
        stream_types=[
            'TRADE_STREAM',
            'DIFFERENCE_DEPTH_STREAM',
            'DEPTH_SNAPSHOT'
        ],
        should_join_pairs_into_one_csv=False,
        should_join_markets_into_one_csv=False,
        csvs_nest_catalog='C:/Users/daniel/Documents/binance_archival_data/',
        dump_catalog='C:/Users/daniel/Documents/merged_csvs/'
    )
