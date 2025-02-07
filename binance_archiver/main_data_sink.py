import time

from dotenv import load_dotenv

from binance_archiver import load_config_from_json, DataSinkConfig, launch_data_sink

if __name__ == "__main__":

    load_dotenv('binance-archiver.env')
    config_from_json = load_config_from_json(json_filename='production_config.json')

    data_sink_config = DataSinkConfig(
        instruments={
            'spot': config_from_json['instruments']['spot'],
            'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
            'coin_m_futures': config_from_json['instruments']['coin_m_futures']
        },
        time_settings={
            "file_duration_seconds": config_from_json["file_duration_seconds"],
            "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
            "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
        },
        data_save_target=config_from_json['data_save_target']
    )

    data_sink = launch_data_sink(data_sink_config=data_sink_config)

    while not data_sink.global_shutdown_flag.is_set():
        time.sleep(16)

    data_sink.logger.info('the program has ended, exiting')
