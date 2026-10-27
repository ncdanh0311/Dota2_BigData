"""Event contracts and atomic serving snapshots. Standard library only."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid

from web.snapshot import PROJECT, safe_id

VERSION = 'replay-v1'
FIELDS = {'schema_version', 'replay_run_id', 'event_id', 'match_id', 'radiant_heroes',
          'dire_heroes', 'radiant_win', 'emitted_at', 'source_mode'}


def now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def envelope(payload):
    return {'payload': payload, 'sha256': digest(canonical(payload))}


def unpack(value):
    data = json.loads(value)
    if not isinstance(data,dict) or not isinstance(data.get('payload'),dict) or data.get('sha256') != digest(canonical(data['payload'])):
        raise ValueError('Streaming snapshot checksum mismatch.')
    return data['payload']


def atomic_publish(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name('.'+path.name+'.'+uuid.uuid4().hex)
    try:
        with temp.open('wb') as stream:
            stream.write(canonical(envelope(payload)))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def draft(record):
    match_id, label = record['match_id'], record['radiant_win']
    radiant, dire = record['radiant_heroes'], record['dire_heroes']
    if type(match_id) is not int or match_id <= 0 or type(label) is not int or label not in (0, 1):
        raise ValueError('Invalid match ID or label.')
    if any(not isinstance(team, list) or len(team) != 5 or
           any(type(h) is not int or h <= 0 for h in team) for team in (radiant, dire)):
        raise ValueError('Invalid hero IDs.')
    if len(set(radiant+dire)) != 10:
        raise ValueError('Duplicate heroes.')
    return {'match_id': match_id, 'radiant_heroes': sorted(radiant),
            'dire_heroes': sorted(dire), 'radiant_win': label}


def validate_event(raw, key, run_id, catalog):
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeError):
        return None, 'invalid_json'
    if not isinstance(event, dict) or set(event) != FIELDS or type(event.get('schema_version')) is not int or event['schema_version'] != 1:
        return None, 'invalid_schema'
    if event['replay_run_id'] != run_id or event['source_mode'] != 'historical_replay':
        return None, 'invalid_run'
    try:
        normalized = draft(event)
    except (ValueError, KeyError, TypeError):
        return None, 'invalid_draft'
    if event['event_id'] != f'{run_id}:{normalized["match_id"]}':
        return None, 'invalid_event_id'
    if key != event['event_id']:
        return None, 'invalid_key'
    try:
        emitted = datetime.fromisoformat(event['emitted_at'])
        if emitted.utcoffset() is None or emitted.utcoffset().total_seconds() != 0:
            raise ValueError('UTC required')
    except (ValueError, TypeError, AttributeError):
        return None, 'invalid_emitted_at'
    expected = catalog.get(normalized['match_id'])
    if expected is None:
        return None, 'not_in_catalog'
    if normalized != expected:
        return None, 'source_mismatch'
    return normalized, 'valid'


def event_for(run_id, record):
    record = draft(record)
    return {**record, 'schema_version': 1, 'replay_run_id': run_id,
            'event_id': f'{run_id}:{record["match_id"]}', 'emitted_at': now(),
            'source_mode': 'historical_replay'}


def local_run(run_id):
    return PROJECT/'artifacts/replay'/safe_id(run_id)


def load_catalog(run_id):
    path = local_run(run_id)
    if not (path/'_COMPLETED').is_file():
        raise ValueError('Replay catalog is incomplete.')
    manifest = json.loads((path/'manifest.json').read_bytes())
    raw = (path/'matches.jsonl').read_bytes()
    if manifest['run_id'] != run_id or digest(raw) != manifest['catalog_sha256']:
        raise ValueError('Replay catalog identity/checksum mismatch.')
    catalog = {row['match_id']: row for row in [draft(json.loads(line)) for line in raw.splitlines()]}
    if len(catalog) != manifest['expected_matches'] or len(raw.splitlines()) != len(catalog):
        raise ValueError('Replay catalog count mismatch.')
    return manifest, catalog


@contextmanager
def exclusive(path):
    import fcntl
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError('Another process is already using this replay/checkpoint.') from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
