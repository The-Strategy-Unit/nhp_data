import json
from importlib import resources


def load_json(file):
    return json.loads(
        get_reference_file_path(f"{file}.json").read_bytes().decode("utf-8")
    )


def get_reference_file_path(filename):
    return resources.files("nhp.data.raw_data.mitigators.reference_data").joinpath(
        filename
    )
