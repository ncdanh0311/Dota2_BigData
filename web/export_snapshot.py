#!/usr/bin/env python3
"""Export a verified, immutable local serving snapshot from completed HDFS runs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analytics.draft_stats import (KEYS, assert_same_table, build_tables, check_gold,
                                  fs_for, input_inventory, validate_input)
from web.snapshot import (PROJECT, checksum, load_snapshot, read_json, reserve_snapshot,
                          safe_id, verify_sources, write_json)
from web.hero_assets import reuse_heroes


def hdfs_bytes(spark, uri):
    fs, path = fs_for(spark, uri)
    stream = fs.open(path)
    try:
        # ByteArrayOutputStream avoids Java UTF-8 reserialization changing checksums.
        output = spark._jvm.java.io.ByteArrayOutputStream()
        spark._jvm.org.apache.hadoop.io.IOUtils.copyBytes(stream, output, 65536, False)
        return bytes(output.toByteArray())
    finally:
        stream.close()


def completed_run(spark, uri):
    fs, _ = fs_for(spark, uri)
    def exists(name):
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(uri+'/'+name))
    if not exists('_COMPLETED') or exists('_INVALIDATED') or exists('_RUNNING'):
        raise ValueError(f'Source run is incomplete or invalidated: {uri}')


def download(url):
    request = urllib.request.Request(url, headers={'User-Agent': 'Dota2-BigData-Coursework'})
    with urllib.request.urlopen(request, timeout=8) as response:
        return response.read()


def prepare_heroes(path, hero_ids, supported):
    revision = json.loads(download('https://api.github.com/repos/odota/dotaconstants/commits/master'))['sha']
    url = f'https://raw.githubusercontent.com/odota/dotaconstants/{revision}/build/heroes.json'
    raw = download(url)
    metadata = json.loads(raw)
    (path/'hero_metadata.json').write_bytes(raw)
    missing = sorted(set(hero_ids)-{int(key) for key in metadata})
    if missing:
        raise ValueError(f'Hero metadata missing IDs: {missing}')
    images = path/'images'
    images.mkdir()
    (images/'placeholder.svg').write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="144" viewBox="0 0 256 144">'
        '<rect width="256" height="144" fill="#182737"/><path d="M110 35h36l20 37-20 37h-36L90 72z" '
        'fill="#547183"/><path d="m116 55 25 17-25 17z" fill="#aec5d0"/></svg>', encoding='utf-8')

    def one(hero_id):
        record = metadata[str(hero_id)]
        image_path = record['img'].split('?')[0]
        if not image_path.startswith('/apps/dota2/images/dota_react/heroes/') or '..' in image_path:
            raise ValueError(f'Unexpected hero image path: {image_path}')
        error, filename, attempts = None, 'placeholder.svg', 0
        for attempt in range(3):
            attempts = attempt+1
            host = ['cdn.cloudflare.steamstatic.com', 'cdn.akamai.steamstatic.com',
                    'cdn.fastly.steamstatic.com'][attempt]
            try:
                image = download('https://'+host+image_path)
                if not image.startswith(b'\x89PNG\r\n\x1a\n'):
                    raise ValueError('Image is not PNG.')
                filename = f'{hero_id}.png'
                (images/filename).write_bytes(image)
                error = None
                break
            except (OSError, ValueError) as failure:
                error = str(failure)
        return {'id': hero_id, 'name': record['localized_name'], 'internal_name': record['name'],
                'supported': hero_id in supported, 'image_url': '/hero-images/'+filename,
                'image_status': 'cached' if error is None else 'placeholder',
                'image_attempts': attempts, 'image_error': error}

    with ThreadPoolExecutor(max_workers=8) as pool:
        heroes = list(pool.map(one, sorted(hero_ids)))
    write_json(path/'heroes.json', heroes)
    return heroes, {'revision': revision, 'url': url, 'sha256': hashlib.sha256(raw).hexdigest(),
                    'cached_images': sum(h['image_status'] == 'cached' for h in heroes),
                    'placeholder_images': sum(h['image_status'] != 'cached' for h in heroes)}


def execute(spark, args):
    started = time.monotonic()
    safe_id(args.snapshot_id)
    safe_id(args.gold_run_id)
    safe_id(args.model_run_id)
    target = PROJECT/'artifacts/web'/args.snapshot_id
    if target.exists():
        raise FileExistsError(f'Snapshot already exists: {target}')
    gold_uri = args.gold_root.rstrip('/')+'/'+args.gold_run_id
    model_uri = args.model_root.rstrip('/')+'/'+args.model_run_id
    completed_run(spark, gold_uri)
    completed_run(spark, model_uri)
    metadata = spark.read.parquet(gold_uri+'/analysis_runs')
    if metadata.count() != 1:
        raise ValueError('Gold metadata must contain exactly one row.')
    gold = metadata.first().asDict()
    model = json.loads(hdfs_bytes(spark, model_uri+'/manifest.json'))
    bundle_bytes = hdfs_bytes(spark, model_uri+'/model_bundle.json')
    bundle = verify_sources(gold, model, bundle_bytes)
    if gold['analysis_run_id'] != args.gold_run_id or model['run_id'] != args.model_run_id:
        raise ValueError('Source run identity mismatch.')
    silver = spark.read.parquet(gold['input_uri']).cache()
    try:
        n = validate_input(silver)
        inventory = input_inventory(spark, gold['input_uri'])
        fingerprint = hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()
        if fingerprint != gold['input_fingerprint_sha256']:
            raise ValueError('Silver inventory changed since source runs.')
        expected = build_tables(spark, silver, args.gold_run_id, gold['min_hero_matches'], gold['min_pair_matches'])
        checks = check_gold(spark, expected, n, gold['min_hero_matches'], gold['min_pair_matches'])
        tables = {name: spark.read.parquet(gold_uri+'/'+name) for name in expected}
        for name in tables:
            assert_same_table(expected[name], tables[name], KEYS[name])
        dashboard = {name: [row.asDict() for row in frame.orderBy(*KEYS[name]).collect()]
                     for name, frame in tables.items()}
        dashboard['match_overview'] = dashboard['match_overview'][0]
        dashboard.update(analysis_run_id=args.gold_run_id, total_matches=n,
                         min_hero_matches=gold['min_hero_matches'], min_pair_matches=gold['min_pair_matches'])
        reserve_snapshot(target)
        try:
            write_json(target/'dashboard.json', dashboard)
            readback = read_json(target/'dashboard.json')
            for name, frame in tables.items():
                rows = [readback[name]] if name == 'match_overview' else readback[name]
                assert_same_table(frame, spark.createDataFrame(rows, schema=frame.schema), KEYS[name])
            (target/'model_bundle.json').write_bytes(bundle_bytes)
            (target/'model_metrics.json').write_bytes(hdfs_bytes(spark, model_uri+'/metrics.json'))
            hero_ids = set(bundle['hero_ids']) | {r['hero_id'] for r in dashboard['hero_stats']}
            cache_source = getattr(args, 'hero_assets_from', None)
            heroes, asset_info = (reuse_heroes(target, hero_ids, set(bundle['hero_ids']), cache_source)
                                 if cache_source else prepare_heroes(target, hero_ids, set(bundle['hero_ids'])))
            manifest = {'snapshot_id': args.snapshot_id, 'gold_run_id': args.gold_run_id,
                        'model_run_id': args.model_run_id, 'gold_uri': gold_uri, 'model_uri': model_uri,
                        'input_fingerprint_sha256': fingerprint, 'row_counts': checks['row_counts'],
                        'gold_readback_verified': True, 'json_readback_verified': True,
                        'metadata': asset_info, 'created_at_utc': datetime.now(timezone.utc).isoformat(),
                        'elapsed_seconds': round(time.monotonic()-started, 2),
                        'checksums': {str(p.relative_to(target)).replace('\\', '/'): checksum(p)
                                      for p in sorted(target.rglob('*')) if p.is_file() and not p.name.startswith('_')}}
            write_json(target/'manifest.json', manifest)
            load_snapshot(target, require_completed=False)
            evidence = PROJECT/'evidence/step5'/args.snapshot_id
            evidence.mkdir(parents=True, exist_ok=False)
            write_json(evidence/'export_checks.json', {**manifest, 'status': 'PASS'})
            write_json(evidence/'image_status.json', {'metadata': asset_info, 'heroes': heroes})
            (target/'_RUNNING').rename(target/'_COMPLETED')
            load_snapshot(target)
            print(json.dumps({'status': 'PASS', 'snapshot': str(target), 'images': asset_info}))
        except Exception as error:
            (target/'_COMPLETED').unlink(missing_ok=True)
            write_json(target/'_FAILED.json', {'error': str(error)})
            raise
    finally:
        silver.unpersist()


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gold-root', default='hdfs://localhost:9000/dota/analytics/draft_stats_v1')
    parser.add_argument('--model-root', default='hdfs://localhost:9000/dota/models/draft_lr_v1')
    parser.add_argument('--gold-run-id', default='step3-20261026-v2')
    parser.add_argument('--model-run-id', default='step4-20261027-v1')
    parser.add_argument('--snapshot-id', default=datetime.now(timezone.utc).strftime('web-%Y%m%dT%H%M%SZ-')+uuid.uuid4().hex[:6])
    parser.add_argument('--hero-assets-from', type=Path, help='Reuse a completed snapshot instead of downloading display assets')
    args = parser.parse_args()
    spark = SparkSession.builder.appName('DotaWebSnapshot').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        execute(spark, args)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
