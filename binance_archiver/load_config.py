import os
import json


def load_config(json_name: str):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'stock_data_sink_configs', json_name)

    if not os.path.isdir(os.path.join(script_dir, 'stock_data_sink_configs')):
        raise FileNotFoundError(f"Katalog stock_data_sink_configs nie istnieje w {script_dir}.")

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Plik {config_path} nie został znaleziony.")

    with open(config_path, 'r', encoding='utf-8') as config_file:
        return json.load(config_file)