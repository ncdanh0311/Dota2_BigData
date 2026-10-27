#!/usr/bin/env python3
"""Train, evaluate and persist a versioned Logistic Regression run."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import time
from urllib.parse import urlsplit
import uuid

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from pyspark import StorageLevel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession, functions as F
from analytics.draft_stats import (assert_same_table, copy_json, fs_for, git_metadata,
                                   input_inventory, reserve_run, utc_now)
from ml.features import (FEATURE_VERSION, SPLIT_RULE, assign_splits, encode_features,
                         fit_vocabulary, validate_features, validate_input, validate_splits)
from ml.training import FIXED_PARAMS, baseline_scored, evaluate, scored, train_candidates
from ml.export import make_bundle, verify_portable
from ml.predict_draft import load_bundle

DEFAULT_INPUT = 'hdfs://localhost:9000/dota/processed/match_drafts_v1/'
DEFAULT_ROOT = 'hdfs://localhost:9000/dota/models/draft_lr_v1'


def logic_hashes():
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((PROJECT/'ml').glob('*.py'))}


def report_markdown(manifest, metrics, validation):
    lines = ['# Bước 4 — Logistic Regression dự đoán đội hình', '',
             f"Run: `{manifest['run_id']}` · **PASS**", '',
             f"Nguồn: `{manifest['input_uri']}` · Seed: {manifest['seed']} · Feature: {len(manifest['hero_ids'])} hero.", '',
             '| Tập | Dire thắng | Radiant thắng | Tổng |', '|---|---:|---:|---:|']
    for name, counts in manifest['split_counts'].items():
        lines.append(f"| {name} | {counts['0']:,} | {counts['1']:,} | {sum(counts.values()):,} |")
    lines += ['', '## Validation — chọn cấu hình', '',
              '| regParam | Accuracy | ROC-AUC | Log loss | Vòng lặp | Solver |', '|---:|---:|---:|---:|---:|---|']
    for item in validation:
        m = item['validation']
        lines.append(f"| {item['reg_param']} | {m['accuracy']:.4f} | {m['roc_auc']:.4f} | {m['log_loss']:.6f} | {item['iterations']} | {item['solver_status']} |")
    lines += ['', f"Chọn regParam={metrics['selected_reg_param']} theo validation log loss; không refit train+validation.", '',
              '## Test — đánh giá sau khi chốt mô hình', '',
              '| Mô hình | Accuracy | Precision (Radiant) | Recall (Radiant) | F1 (Radiant) | ROC-AUC | Log loss |',
              '|---|---:|---:|---:|---:|---:|---:|']
    for name in ['baseline', 'logistic_regression']:
        m = metrics['test'][name]
        lines.append(f"| {name} | {m['accuracy']:.4f} | {m['precision_radiant']:.4f} | {m['recall_radiant']:.4f} | {m['f1_radiant']:.4f} | {m['roc_auc']:.4f} | {m['log_loss']:.6f} |")
    lines += ['', 'Confusion matrix (TN, FP, FN, TP):', '',
              '```json', json.dumps({name: value['confusion_matrix'] for name, value in metrics['test'].items()}, indent=2),
              '```', '', '## Kiểm chứng và giới hạn', '',
              '- Mapping chỉ fit trên train; holdout không chứa hero ngoài mapping.',
              '- Chọn tham số bằng validation; test không dùng để tuning hoặc đổi threshold.',
              '- Chia theo hash match ID, không phải theo thời gian/patch.',
              '- Baseline dùng prior Radiant chỉ từ train; tỷ lệ Gold không tham gia feature.',
              '- Số liệu là holdout trong cùng dataset, không bảo đảm hiệu quả meta hiện tại.',
              f"- So sánh baseline: `{metrics['comparison_to_baseline']}`.",
              f"- Thời gian xử lý: {manifest['elapsed_seconds']} giây.", '']
    return '\n'.join(lines)


def execute(spark, args):
    started = time.perf_counter()
    run_uri = args.output_root.rstrip('/') + '/' + args.run_id
    fs, run_path = fs_for(spark, run_uri)
    if fs.exists(run_path):
        raise FileExistsError(f'Run already exists: {run_uri}')
    evidence = PROJECT/'evidence/step4'/args.run_id
    if evidence.exists():
        raise FileExistsError(f'Local evidence already exists: {evidence}')
    local_model = PROJECT/'artifacts/models'/args.run_id
    if local_model.exists():
        raise FileExistsError(f'Local model already exists: {local_model}')
    source = spark.read.parquet(args.input).persist(StorageLevel.MEMORY_AND_DISK)
    assigned = encoded = None
    try:
        n = validate_input(source)
        inventory = input_inventory(spark, args.input)
        assigned = assign_splits(source, args.seed).persist(StorageLevel.MEMORY_AND_DISK)
        split_counts = validate_splits(assigned, n)
        vocabulary = fit_vocabulary(assigned)
        encoded = encode_features(assigned, vocabulary).persist(StorageLevel.MEMORY_AND_DISK)
        validate_features(encoded, len(vocabulary))
        if args.input.rstrip('/') == DEFAULT_INPUT.rstrip('/') and args.seed == 42:
            if split_counts != {'train': {'0': 31009, '1': 32139},
                                'validation': {'0': 6503, '1': 7115}, 'test': {'0': 6713, '1': 7051}}:
                raise ValueError('Split counts differ from the verified Silver v1 reference.')
        print(f'Validated {n:,} matches, {len(vocabulary)} train-only heroes. Splits: {split_counts}', flush=True)
        reserve_run(spark, run_uri)
        evidence.mkdir(parents=True)
        try:
            assignments = assigned.select('match_id', 'bucket', 'split')
            assignments.repartition(2).write.mode('errorifexists').option('compression', 'snappy').parquet(run_uri+'/split_assignments')
            assert_same_table(assignments, spark.read.parquet(run_uri+'/split_assignments'), ['match_id'])
            data = {name: encoded.where(F.col('split') == name) for name in ['train', 'validation', 'test']}
            counts = split_counts['train']
            prior = counts['1']/sum(counts.values())
            baseline_validation = evaluate(baseline_scored(data['validation'], prior))
            model, selected, candidates = train_candidates(data['train'], data['validation'])
            # Test is touched for scoring only after candidate selection has finished.
            test_predictions = scored(model, data['test']).cache()
            try:
                test_model = evaluate(test_predictions)
                test_baseline = evaluate(baseline_scored(data['test'], prior))
                model.write().save(run_uri+'/spark_model')
                reloaded = LogisticRegressionModel.load(run_uri+'/spark_model')
                assert_same_table(test_predictions, scored(reloaded, data['test']), ['match_id'])
                bundle = make_bundle(reloaded, vocabulary, args.run_id)
                local_model.mkdir(parents=True)
                bundle_path = local_model/'model_bundle.json'
                bundle_path.write_text(json.dumps(bundle, sort_keys=True, indent=2), encoding='utf-8')
                loaded_bundle = load_bundle(bundle_path)
                portable_check = verify_portable(test_predictions, data['test'], loaded_bundle)
                bundle_hash = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
                copy_json(spark, bundle_path, run_uri+'/model_bundle.json')
                test_predictions.coalesce(1).write.mode('errorifexists').option('compression', 'snappy').parquet(run_uri+'/test_predictions')
                assert_same_table(test_predictions, spark.read.parquet(run_uri+'/test_predictions'), ['match_id'])
                examples = data['test'].select('match_id', 'radiant_heroes', 'dire_heroes').join(
                    test_predictions, 'match_id').orderBy('match_id').limit(10).collect()
            finally:
                test_predictions.unpersist()
            metrics = {'selected_reg_param': selected['reg_param'], 'threshold': 0.5,
                       'baseline_train_prior': prior,
                       'validation': {'baseline': baseline_validation, 'logistic_regression': selected['validation']},
                       'test': {'baseline': test_baseline, 'logistic_regression': test_model},
                       'comparison_to_baseline': {
                           'accuracy_delta': test_model['accuracy']-test_baseline['accuracy'],
                           'log_loss_improvement': test_baseline['log_loss']-test_model['log_loss']}}
            manifest = {'status': 'PASS', 'run_id': args.run_id, 'input_uri': args.input, 'output_uri': run_uri,
                        'created_at_utc': utc_now(), 'spark_version': spark.version, **git_metadata(),
                        'logic_hashes': logic_hashes(), 'seed': args.seed, 'split_rule': SPLIT_RULE,
                        'input_matches': n, 'input_files': inventory,
                        'input_fingerprint_sha256': hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest(),
                        'split_counts': split_counts, 'hero_ids': vocabulary,
                        'feature_version': FEATURE_VERSION, 'feature_dimension': len(vocabulary),
                        'unknown_holdout_heroes': 0, 'fixed_parameters': FIXED_PARAMS,
                        'selected_reg_param': selected['reg_param'],
                        'native_model_readback_verified': True, 'prediction_parquet_readback_verified': True,
                        'portable_parity': portable_check, 'model_bundle_sha256': bundle_hash,
                        'local_bundle_path': str(bundle_path),
                        'elapsed_seconds': round(time.perf_counter()-started, 2),
                        'limitations': ['Hash holdout, not temporal or patch validation.',
                                        'Draft-only observational data; player skill, team identity and meta absent.']}
            payloads = {'manifest.json': manifest, 'metrics.json': metrics, 'validation_metrics.json': candidates,
                        'prediction_examples.json': [row.asDict() for row in examples]}
            for name, value in payloads.items():
                (evidence/name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
                copy_json(spark, evidence/name, run_uri+'/'+name)
            (evidence/'report.md').write_text(report_markdown(manifest, metrics, candidates), encoding='utf-8')
            (evidence/'portable_parity.json').write_text(json.dumps(portable_check, indent=2), encoding='utf-8')
            copy_json(spark, evidence/'report.md', run_uri+'/report.md')
            copy_json(spark, evidence/'portable_parity.json', run_uri+'/portable_parity.json')
            if not fs.rename(spark._jvm.org.apache.hadoop.fs.Path(run_uri+'/_RUNNING'),
                             spark._jvm.org.apache.hadoop.fs.Path(run_uri+'/_COMPLETED')):
                raise RuntimeError('Cannot publish completion marker.')
            print(f'PASS: {run_uri}\nReport: {evidence/"report.md"}', flush=True)
            print(json.dumps(metrics, indent=2), flush=True)
        except Exception as error:
            failure = evidence/'failure.json'
            failure.write_text(json.dumps({'status': 'FAILED', 'error': str(error)}, indent=2), encoding='utf-8')
            try:
                copy_json(spark, failure, run_uri+'/_FAILED.json')
            except Exception:
                pass
            raise
    finally:
        for frame in [encoded, assigned, source]:
            if frame is not None:
                frame.unpersist()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default=DEFAULT_INPUT)
    parser.add_argument('--output-root', default=DEFAULT_ROOT)
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8])
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    root = urlsplit(args.output_root)
    if root.scheme != 'hdfs' or not root.path.startswith('/dota/models/') or '..' in PurePosixPath(root.path).parts:
        parser.error('--output-root must be an HDFS URI under /dota/models/.')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', args.run_id):
        parser.error('Run ID must be a safe single directory name.')
    return args


def main():
    args = arguments()
    spark = (SparkSession.builder.appName('Dota-Step4-Logistic-Regression')
             .config('spark.sql.shuffle.partitions', '8').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        execute(spark, args)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
