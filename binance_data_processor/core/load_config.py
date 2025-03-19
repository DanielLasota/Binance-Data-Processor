import os
import json


def load_config_from_json(json_filename: str) -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_dir = script_dir.replace(os.sep + 'core', '')
    config_path = os.path.join(script_dir, 'stock_data_sink_configs', json_filename)

    if not os.path.isdir(os.path.join(script_dir, 'stock_data_sink_configs')):
        raise FileNotFoundError(f"Catalog stock_data_sink_configs doesnt exist {script_dir}.")

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"File {config_path} not found.")

    with open(config_path, 'r', encoding='utf-8') as config_file:
        return json.load(config_file)
