from binance_data_processor import launch_data_listener, DataSinkConfig


class SampleObserverClass:
    @staticmethod
    def update(message):
        print(f"message: {message}")

if __name__ == '__main__':
    sample_observer = SampleObserverClass()

    data_listener = launch_data_listener(
        data_sink_config=DataSinkConfig(show_logo=False),
        observers=[sample_observer]
    )
