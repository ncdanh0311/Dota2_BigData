#!/usr/bin/env python3
"""Portable draft inference. Standard library only; never imports Spark."""
import argparse
import json
import math
from pathlib import Path
import re
import sys


def validate_bundle(bundle):
    if bundle.get('feature_version') != 'signed-hero-v1':
        raise ValueError('Unsupported feature version.')
    if not isinstance(bundle.get('run_id'), str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', bundle['run_id']):
        raise ValueError('Invalid model run ID.')
    heroes, coefficients = bundle.get('hero_ids'), bundle.get('coefficients')
    if (not isinstance(heroes, list) or len(heroes) < 10 or
        any(type(hero) is not int or hero <= 0 for hero in heroes) or heroes != sorted(set(heroes))):
        raise ValueError('Invalid hero mapping.')
    if (not isinstance(coefficients, list) or len(coefficients) != len(heroes) or
        any(type(value) not in (float, int) or not math.isfinite(value) for value in coefficients)):
        raise ValueError('Invalid coefficients.')
    intercept = bundle.get('intercept')
    if type(intercept) not in (int, float) or not math.isfinite(intercept):
        raise ValueError('Invalid intercept.')
    if bundle.get('threshold') != 0.5 or bundle.get('class_mapping') != {'0': 'dire', '1': 'radiant'}:
        raise ValueError('Unsupported class mapping or threshold.')
    return bundle


def stable_sigmoid(value):
    if value >= 0:
        return 1.0/(1.0+math.exp(-value))
    exponential = math.exp(value)
    return exponential/(1.0+exponential)


def predict(bundle, radiant, dire):
    validate_bundle(bundle)
    for heroes in [radiant, dire]:
        if not isinstance(heroes, (list, tuple)) or len(heroes) != 5 or any(type(hero) is not int or hero <= 0 for hero in heroes):
            raise ValueError('Each side requires exactly five positive integer hero IDs.')
    if len(set([*radiant, *dire])) != 10:
        raise ValueError('Heroes must be unique within and between both teams.')
    weights = dict(zip(bundle['hero_ids'], bundle['coefficients']))
    unknown = sorted(set([*radiant, *dire])-set(weights))
    if unknown:
        raise ValueError(f'Unsupported heroes: {unknown}')
    logit = math.fsum([bundle['intercept'], *[weights[hero] for hero in radiant],
                      *[-weights[hero] for hero in dire]])
    if not math.isfinite(logit):
        raise ValueError('Nonfinite draft logit.')
    p = stable_sigmoid(logit)
    return {'run_id': bundle['run_id'], 'radiant_heroes': sorted(radiant),
            'dire_heroes': sorted(dire), 'p_radiant': p, 'p_dire': 1-p,
            'prediction': int(p > bundle['threshold']),
            'predicted_side': 'radiant' if p > bundle['threshold'] else 'dire'}


def load_bundle(path):
    return validate_bundle(json.loads(Path(path).read_text(encoding='utf-8')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--radiant', required=True, help='Five comma-separated hero IDs')
    parser.add_argument('--dire', required=True, help='Five comma-separated hero IDs')
    args = parser.parse_args()
    try:
        rad = [int(value.strip()) for value in args.radiant.split(',')]
        dire = [int(value.strip()) for value in args.dire.split(',')]
        print(json.dumps(predict(load_bundle(args.bundle), rad, dire), indent=2))
    except (ValueError, TypeError, OSError, OverflowError) as error:
        print(f'Prediction failed: {error}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
