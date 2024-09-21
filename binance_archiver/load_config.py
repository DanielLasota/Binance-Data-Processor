import json

def load_config(json_name: str):
    with open(json_name, 'r') as config_file:
        return json.load(config_file)
