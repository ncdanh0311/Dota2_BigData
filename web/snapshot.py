"""Snapshot integrity checks. Standard library only, including at web startup."""
import hashlib
import json
from pathlib import Path
import re

from ml.predict_draft import validate_bundle

PROJECT = Path(__file__).resolve().parents[1]


def safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', value):
        raise ValueError('Invalid snapshot/run ID.')
    return value


def checksum(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_json(path, data):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')


def reserve_snapshot(path):
    Path(path).mkdir(parents=True, exist_ok=False)
    (Path(path)/'_RUNNING').touch(exist_ok=False)


def verify_sources(gold, model, bundle_bytes):
    if gold['input_fingerprint_sha256'] != model['input_fingerprint_sha256']:
        raise ValueError('Gold and model Silver fingerprints differ.')
    if hashlib.sha256(bundle_bytes).hexdigest() != model['model_bundle_sha256']:
        raise ValueError('Model bundle checksum mismatch.')
    bundle = validate_bundle(json.loads(bundle_bytes))
    if bundle['run_id'] != model['run_id']:
        raise ValueError('Model run ID mismatch.')
    return bundle


def load_snapshot(path, require_completed=True):
    path = Path(path).resolve()
    if require_completed and (not (path/'_COMPLETED').is_file() or
                              (path/'_INVALIDATED').exists() or (path/'_RUNNING').exists()):
        raise ValueError('Snapshot is incomplete or invalidated.')
    manifest = read_json(path/'manifest.json')
    safe_id(manifest['snapshot_id'])
    required = {'dashboard.json', 'heroes.json', 'model_bundle.json', 'model_metrics.json',
                'hero_metadata.json', 'images/placeholder.svg'}
    inventory = manifest['checksums']
    if not required.issubset(inventory):
        raise ValueError('Snapshot inventory is incomplete.')
    for relative, expected in inventory.items():
        target = (path/relative).resolve()
        if not target.is_relative_to(path) or not target.is_file() or checksum(target) != expected:
            raise ValueError(f'Snapshot checksum mismatch: {relative}')
    bundle = validate_bundle(read_json(path/'model_bundle.json'))
    if bundle['run_id'] != manifest['model_run_id']:
        raise ValueError('Snapshot model ID mismatch.')
    dashboard, heroes = read_json(path/'dashboard.json'), read_json(path/'heroes.json')
    if dashboard['analysis_run_id'] != manifest['gold_run_id']:
        raise ValueError('Snapshot Gold ID mismatch.')
    for name, count in manifest['row_counts'].items():
        actual = 1 if name == 'match_overview' else len(dashboard[name])
        if count != actual:
            raise ValueError(f'Snapshot row count mismatch: {name}')
    ids = [hero['id'] for hero in heroes]
    supported = {hero['id'] for hero in heroes if hero['supported']}
    if len(ids) != len(set(ids)) or supported != set(bundle['hero_ids']):
        raise ValueError('Snapshot hero coverage mismatch.')
    for hero in heroes:
        relative = hero['image_url'].removeprefix('/hero-images/')
        if hero['image_url'] != '/hero-images/'+relative or 'images/'+relative not in inventory:
            raise ValueError('Hero image is missing from snapshot inventory.')
    return {'manifest': manifest, 'dashboard': dashboard, 'heroes': heroes,
            'bundle': bundle, 'metrics': read_json(path/'model_metrics.json')}
