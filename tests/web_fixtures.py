"""Small local-only web fixtures; never presented as real model results."""
import base64
from web.snapshot import checksum, write_json

PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=')


def bundle():
    return {'run_id': 'fixture-model', 'feature_version': 'signed-hero-v1', 'hero_ids': list(range(1, 11)),
            'coefficients': [0.1*i for i in range(10)], 'intercept': 0.0, 'threshold': 0.5,
            'class_mapping': {'0': 'dire', '1': 'radiant'}}


def make_snapshot(root):
    root.mkdir()
    (root/'images').mkdir()
    (root/'images/placeholder.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    write_json(root/'model_bundle.json', bundle())
    write_json(root/'model_metrics.json', {'test': {}})
    write_json(root/'hero_metadata.json', {'fixture': True})
    write_json(root/'heroes.json', [{'id': i, 'name': f'Hero {i}', 'internal_name': f'hero_{i}',
                                    'image_url': '/hero-images/placeholder.svg', 'supported': True}
                                   for i in range(1, 11)])
    write_json(root/'dashboard.json', {'analysis_run_id': 'fixture-gold', 'total_matches': 2,
        'match_overview': {'total_matches': 2, 'radiant_wins': 1, 'dire_wins': 1,
                           'radiant_win_rate': 0.5, 'dire_win_rate': 0.5},
        'hero_stats': [], 'hero_side_stats': [], 'hero_pair_stats': [],
        'min_hero_matches': 100, 'min_pair_matches': 100})
    write_json(root/'manifest.json', {'snapshot_id': 'fixture-web', 'gold_run_id': 'fixture-gold',
        'model_run_id': 'fixture-model', 'row_counts': {'match_overview': 1, 'hero_stats': 0,
            'hero_side_stats': 0, 'hero_pair_stats': 0},
        'checksums': {str(p.relative_to(root)): checksum(p) for p in root.rglob('*') if p.is_file()}})
    (root/'_COMPLETED').touch()
    return root
