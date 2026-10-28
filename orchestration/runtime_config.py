"""Explicit runtime overrides for ephemeral Linux CI; WSL remains the default."""
import json
import os
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


def load_config(service, project=PROJECT):
    if service not in {'hdfs', 'kafka', 'airflow'}:
        raise ValueError('Unknown runtime service.')
    path = Path(os.environ.get(f'DOTA_{service.upper()}_CONFIG',
                               str(Path(project)/'configs'/f'{service}-wsl.json')))
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError('Runtime config must be a JSON object.')
    return value
