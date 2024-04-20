import json
import os
import time
from orderbook_level_2_listener.orderbook_level_2_listener import Level2OrderbookDaemon
from orderbook_level_2_listener.market_enum import Market
from dotenv import load_dotenv


class DaemonManager:
    def __init__(
            self,
            config_path: str,
            env_path: str,
            dump_path: str = ''
    ) -> None:
        self.config_path = config_path
        self.env_path = env_path
        self.dump_path = dump_path
        self.daemons = []

    def load_config(self):
        with open(self.config_path, 'r') as file:
            return json.load(file)

    def start_daemons(self):

        if not os.path.exists(self.dump_path) or self.dump_path == '':
            os.makedirs(self.dump_path)

        load_dotenv(self.env_path)

        config = self.load_config()
        for entry in config['daemons']:
            daemon = Level2OrderbookDaemon(
                azure_blob_parameters_with_key=os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY'),
                container_name=os.environ.get('CONTAINER_NAME')
            )
            daemon.run(
                instrument=entry['instrument'],
                market=Market[entry['market']],
                single_file_listen_duration_in_seconds=entry['listen_duration'],
                dump_path=self.dump_path
            )
            self.daemons.append(daemon)

    def stop_daemons(self):
        for daemon in self.daemons:
            # daemon.close_all() # under implementation
            pass
        print("Stopped all daemons")

    def run(self):
        self.start_daemons()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_daemons()