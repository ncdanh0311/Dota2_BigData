#!/usr/bin/env python3
"""End-to-end export on local Spark Parquet; network fully mocked."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pyspark.sql import SparkSession, types as T
from analytics.draft_stats import build_tables, input_inventory
from web.export_snapshot import completed_run, execute
from web.snapshot import load_snapshot, verify_sources, write_json
from web_fixtures import PNG, bundle


def rejected(call, kind=ValueError):
    try:
        call()
    except kind:
        return
    raise AssertionError('Expected rejection')


def main():
    spark = SparkSession.builder.master('local[2]').appName('Web-export-fixture').config('spark.sql.shuffle.partitions', '2').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            silver = root/'silver'
            schema = 'match_id long, radiant_heroes array<int>, dire_heroes array<int>, radiant_win int'
            df = spark.createDataFrame([(i, [1,2,3,4,5], [6,7,8,9,10], i%2) for i in range(1, 5)], schema)
            df.write.parquet(str(silver))
            gold, model = root/'gold/fixture-gold', root/'models/fixture-model'
            for name, frame in build_tables(spark, df, 'fixture-gold').items():
                frame.write.parquet(str(gold/name))
            fingerprint = hashlib.sha256(json.dumps(input_inventory(spark, str(silver)), sort_keys=True).encode()).hexdigest()
            meta = {'analysis_run_id': 'fixture-gold', 'input_uri': str(silver),
                    'input_fingerprint_sha256': fingerprint, 'min_hero_matches': 100, 'min_pair_matches': 100}
            spark.createDataFrame([meta]).write.parquet(str(gold/'analysis_runs'))
            (gold/'_COMPLETED').touch()
            model.mkdir(parents=True)
            write_json(model/'model_bundle.json', bundle())
            manifest = {'input_fingerprint_sha256': fingerprint, 'run_id': 'fixture-model',
                'model_bundle_sha256': hashlib.sha256((model/'model_bundle.json').read_bytes()).hexdigest()}
            write_json(model/'manifest.json', manifest)
            write_json(model/'metrics.json', {'test': {}})
            rejected(lambda: completed_run(spark, str(model)))
            (model/'_COMPLETED').touch()
            (model/'_INVALIDATED').touch()
            rejected(lambda: completed_run(spark, str(model)))
            (model/'_INVALIDATED').unlink()
            rejected(lambda: verify_sources(meta, {**manifest, 'input_fingerprint_sha256': 'wrong'}, b'{}'))
            rejected(lambda: verify_sources(meta, manifest, b'{}'))
            assets = {str(i): {'id': i, 'localized_name': f'Hero {i}', 'name': f'hero_{i}',
                              'img': f'/apps/dota2/images/dota_react/heroes/{i}.png?'} for i in range(1, 11)}
            def download(url):
                if 'commits/' in url:
                    return b'{"sha":"fixture-revision"}'
                if url.endswith('heroes.json'):
                    return json.dumps(assets).encode()
                if url.endswith('/10.png'):
                    raise OSError('Fixture image failure')
                return PNG
            args = argparse.Namespace(snapshot_id='fixture-web', gold_run_id='fixture-gold',
                model_run_id='fixture-model', gold_root=str(root/'gold'), model_root=str(root/'models'))
            with patch('web.export_snapshot.PROJECT', root), patch('web.export_snapshot.download', download):
                execute(spark, args)
                rejected(lambda: execute(spark, args), FileExistsError)
                args.snapshot_id = 'failed-web'
                with patch('web.export_snapshot.prepare_heroes', side_effect=OSError('Fixture metadata outage')):
                    rejected(lambda: execute(spark, args), OSError)
                failed = root/'artifacts/web/failed-web'
                assert (failed/'_FAILED.json').is_file() and not (failed/'_COMPLETED').exists()
            snapshot = root/'artifacts/web/fixture-web'
            loaded = load_snapshot(snapshot)
            assert loaded['dashboard']['total_matches'] == 4
            assert loaded['manifest']['metadata']['cached_images'] == 9
            assert loaded['heroes'][-1]['image_attempts'] == 3
            assert loaded['heroes'][-1]['image_status'] == 'placeholder'
            (snapshot/'_COMPLETED').rename(snapshot/'_RUNNING')
            rejected(lambda: load_snapshot(snapshot))
            (snapshot/'_RUNNING').rename(snapshot/'_COMPLETED')
            (snapshot/'dashboard.json').write_text('{}')
            rejected(lambda: load_snapshot(snapshot))
        evidence = Path(__file__).resolve().parents[1]/'evidence/step5'
        evidence.mkdir(exist_ok=True)
        write_json(evidence/'export_tests.json', {'status': 'PASS', 'source': 'local synthetic fixture',
            'checks': ['Gold keyed JSON parity', 'source completion/invalidation', 'fingerprint/checksum rejection',
                       'no overwrite', 'asset retry/fallback', 'snapshot integrity', 'failed run has no completion marker']})
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
