"""Offline display-asset reuse; independent of Spark and model training."""
from pathlib import Path
import shutil
from web.snapshot import load_snapshot, checksum, write_json


def reuse_heroes(path, hero_ids, supported, source):
    path, source = Path(path), Path(source).resolve()
    previous = load_snapshot(source)
    by_id = {row['id']: row for row in previous['heroes']}
    missing = sorted(set(hero_ids)-set(by_id))
    if missing:
        raise ValueError(f'Cached hero assets missing IDs: {missing}')
    images = path/'images'
    images.mkdir()
    shutil.copy2(source/'hero_metadata.json', path/'hero_metadata.json')
    shutil.copy2(source/'images/placeholder.svg', images/'placeholder.svg')
    heroes = []
    for hero_id in sorted(hero_ids):
        row = {**by_id[hero_id], 'supported': hero_id in supported, 'image_attempts_in_this_export': 0}
        filename = row['image_url'].removeprefix('/hero-images/')
        if filename != 'placeholder.svg':
            shutil.copy2(source/'images'/filename, images/filename)
        heroes.append(row)
    write_json(path/'heroes.json', heroes)
    return heroes, {**previous['manifest'].get('metadata', {}),
        'asset_source_snapshot': previous['manifest']['snapshot_id'],
        'asset_source_manifest_sha256': checksum(source/'manifest.json'),
        'cached_images': sum(row.get('image_status') == 'cached' for row in heroes),
        'placeholder_images': sum(row.get('image_status') != 'cached' for row in heroes)}
