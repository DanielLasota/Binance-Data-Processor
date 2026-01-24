from binance_data_processor import launch_data_listener
from binance_data_processor import DataSinkCfg


class SampleObserverClass:
    @staticmethod
    def update(message):
        print(f"message: {message}")

if __name__ == '__main__':
    sample_observer = SampleObserverClass()

    listener = launch_data_listener(DataSinkCfg(show_logo=False), observers=[sample_observer])
