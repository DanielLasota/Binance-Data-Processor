from binance_archiver import launch_data_listener, DataSinkConfig
from binance_archiver.abstract_base_classes import Observer
from load_config import load_config_from_json


class ConcreteObserver(Observer):
    def update(self, message):
        print(f"message: {message}")
        ...

if __name__ == '__main__':
    sample_observer = ConcreteObserver()

    config_from_json = load_config_from_json(json_filename='almost_production_config.json')

    data_sink_config = DataSinkConfig(
        instruments={
            'spot': config_from_json['instruments']['spot'],
            'usd_m_futures': config_from_json['instruments']['usd_m_futures'],
            'coin_m_futures': config_from_json['instruments']['coin_m_futures']
        },
        time_settings={
            "snapshot_fetcher_interval_seconds": config_from_json["snapshot_fetcher_interval_seconds"],
            "websocket_life_time_seconds": config_from_json["websocket_life_time_seconds"]
        },
    )

    data_listener = launch_data_listener(
        data_sink_config=data_sink_config,
        init_observers=[sample_observer]
    )
