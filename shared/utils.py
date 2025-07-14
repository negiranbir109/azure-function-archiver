import json
import os

def load_env_config(config_path):
    with open(config_path) as f:
        config = json.load(f)
        os.environ.update({k: str(v) for k, v in config.items()})

def load_archive_map(path="archive_map.json"):
    with open(path) as f:
        return json.load(f)

def resolve_archive_dest(blob_name, archive_map):
    fname = blob_name.lower()
    for prefix, (container_key, subfolder) in archive_map.items():
        if fname.startswith(prefix):
            container = os.environ.get(container_key)
            if container:
                return container, subfolder
            else:
                raise ValueError(f"Missing env key: {container_key}")
    return os.environ["DefaultArchiveContainer"], "misc"