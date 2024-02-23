from DiscordAlertsTrader.configurator import cfg
import os.path as op

data_dir = cfg["general"]["data_dir"]
json_file_1 = op.join(data_dir, "exported/demon_export_2023-06-01_2024-01-01.json")
json_file_2 = op.join(data_dir, "exported/demon_export_2024-01-01_2024-02-01.json")
json_file_3 = op.join(data_dir, "exported/demon_export_2024-02-01.json")

json_files = [json_file_1, json_file_2, json_file_3]

import json
from datetime import datetime
import pandas as pd


def load_json_file(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def merge_json_files(json_files):
    # merge the messages field which is a list of messages
    # modify the "dateRange" field
    data = load_json_file(json_files[0])
    for json_file in json_files[1:]:
        data_to_merge = load_json_file(json_file)
        data["messages"] += data_to_merge["messages"]
        data["dateRange"]["before"] = data_to_merge["dateRange"]["before"]

    return data


def save_json_file(data, json_file):
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


data = merge_json_files(json_files)
json_file = op.join(data_dir, "exported/demon_export_2023-06-01.json")
save_json_file(data, json_file)

# Load the merged json file
data = load_json_file(json_file)

# Convert the json data to a pandas dataframe
df = pd.json_normalize(data)
print(df.head())
