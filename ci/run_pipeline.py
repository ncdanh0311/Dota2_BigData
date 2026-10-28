"""Trigger real Airflow DAGs and collect business outputs from this CI run."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import quote

from orchestration.airflow_run_control import api, token
from orchestration.verify_pipeline_runs import verify_run
from orchestration.workflow_job_runner import identity, read, write
from orchestration.runtime_config import load_config
from web.snapshot import PROJECT, load_snapshot

OUT = PROJECT/'artifacts/integration'


def run_dag(dag, run_id, conf, expected='success'):
    access = token()
    endpoint = 'dags/'+dag
    for _ in range(90):
        try:
            api(access, endpoint, 'PATCH', {'is_paused': False})
            break
        except RuntimeError:
            time.sleep(2)
    else:
        raise TimeoutError('DAG was not registered by the real scheduler.')
    api(access, endpoint+'/dagRuns', 'POST', {'dag_run_id': run_id, 'logical_date': None, 'conf': conf})
    for _ in range(240):
        result = api(access, endpoint+'/dagRuns/'+quote(run_id, safe=''))
        if result['state'] in {'success', 'failed'}:
            report, state = verify_run(access, dag, run_id, expected)
            write(OUT/'reports'/f'{run_id}-tasks.json', state)
            return report, state
        time.sleep(10)
    raise TimeoutError(f'DAG timeout: {run_id}')


def hdfs(*args):
    runtime = load_config('hdfs')
    return subprocess.run([runtime['hadoop_home']+'/bin/hdfs', '--config',
        runtime['cluster_root']+'/conf/namenode', *map(str, args)], check=True, capture_output=True, text=True)


def export_hdfs(uri, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    hdfs('dfs', '-get', uri, path)


def batch(run_id):
    failed, failure_state = run_dag('dota_batch_pipeline', run_id+'-preflight',
        {'raw_input': 'hdfs://localhost:9000/dota/raw/ci-file-missing.csv', 'assets_snapshot_id': ''}, 'failed')
    report, state = run_dag('dota_batch_pipeline', run_id,
                          {'assets_snapshot_id': ''})
    chain = report['summary']['source_chain']
    silver, gold, model = (read(chain[key][file]) for key, file in [
        ('normalize_silver', 'report_path'), ('analyze_gold', 'report_path'), ('train_lr', 'manifest_path')])
    snapshot = load_snapshot(chain['export_snapshot']['path'])
    if (silver['metrics']['source_rows'], silver['metrics']['valid_matches']) != (1007768, 90530):
        raise ValueError('Full-source accounting differs from the verified dataset.')
    if not silver['parquet_readback_verified'] or not gold['gold_readback_verified'] or not model['native_model_readback_verified']:
        raise ValueError('Readback verification missing.')
    if not (gold['input_fingerprint_sha256'] == model['input_fingerprint_sha256'] == snapshot['manifest']['input_fingerprint_sha256']):
        raise ValueError('Silver/Gold/model/website lineage mismatch.')
    export_hdfs(chain['normalize_silver']['uri'], OUT/'silver/matches')
    gold_uri = 'hdfs://localhost:9000/dota/analytics/draft_stats_v1/'+chain['analyze_gold']['run_id']
    export_hdfs(gold_uri, OUT/'reports/gold')
    shutil.copytree(chain['export_snapshot']['path'], OUT/'reports/website')
    for name in ['metrics.json', 'manifest.json', 'validation_metrics.json', 'report.md']:
        shutil.copy2(Path(chain['train_lr']['manifest_path']).parent/name, OUT/'reports'/('model-'+name))
    for name in ['report.json', 'quality_checks.json', 'samples.json', 'report.md']:
        shutil.copy2(Path(chain['analyze_gold']['report_path']).parent/name, OUT/'reports'/('gold-'+name))
    write(OUT/'reports/batch.json', {'status': 'PASS', 'source_kind': 'github_integration',
        'source': 'Full Zenodo CSV; native HDFS (3 DataNodes), Spark and real Airflow scheduler',
        'github_run_id': os.environ['GITHUB_RUN_ID'], 'git_commit': os.environ['GITHUB_SHA'],
        'raw_rows': silver['metrics']['source_rows'], 'matches': model['input_matches'],
        'silver_uri': chain['normalize_silver']['uri'], 'gold': gold['quality_checks'],
        'model': snapshot['metrics'], 'split_counts': model['split_counts'],
        'snapshot_id': snapshot['manifest']['snapshot_id'], 'metadata': snapshot['manifest']['metadata'],
        'fingerprint': model['input_fingerprint_sha256'], 'airflow': state,
        'expected_preflight_failure': failure_state})
    write(OUT/'reports/silver-transfer.json', {'matches': 90530,
        'fingerprint': model['input_fingerprint_sha256'], 'source_run_id': os.environ['GITHUB_RUN_ID']})


def replay(run_id):
    from ci.publish_results import verify_artifact
    verify_artifact(OUT/'baseline/silver')
    verify_artifact(OUT/'baseline/reports')
    transfer = read(OUT/'baseline/reports/silver-transfer.json')
    if str(transfer['source_run_id']) != os.environ['GITHUB_RUN_ID']:
        raise ValueError('Silver artifact belongs to a different workflow run.')
    hdfs('dfs', '-mkdir', '-p', '/dota/processed')
    hdfs('dfs', '-put', OUT/'baseline/silver/matches', '/dota/processed/match_drafts_v1')
    report, state = run_dag('dota_replay_demo', run_id,
        {'limit': 1000, 'rate': 100, 'demo_fail_once': True})
    summary = report['summary']
    if [summary[key] for key in ['total_records', 'unique_matches', 'duplicate_records', 'quarantined_records']] != [2000, 1000, 1000, 0]:
        raise ValueError('Actual Kafka retry/deduplication counts differ.')
    producer = next(row for row in state['tasks'] if row['task_id'] == 'produce_events')
    if producer['try_number'] != 2:
        raise ValueError('Producer did not retry through Airflow.')
    replay_id = summary['run_id']
    export_hdfs('hdfs://localhost:9000/dota/streaming/replay_v1/'+replay_id, OUT/'streaming/hdfs-state')
    shutil.copytree(PROJECT/'artifacts/streaming'/replay_id, OUT/'streaming/local-state')
    from streaming.replay_event_contract import unpack
    latest = unpack((OUT/'streaming/local-state/latest.json').read_bytes())
    write(OUT/'reports/replay.json', {'status': 'PASS', 'source_kind': 'github_integration',
        'source': 'Real Kafka broker, Airflow retry and Spark Structured Streaming on HDFS',
        'github_run_id': os.environ['GITHUB_RUN_ID'], 'git_commit': os.environ['GITHUB_SHA'],
        'baseline_matches': transfer['matches'], 'airflow': state, 'summary': summary,
        'overview': latest['match_overview'], 'hero_picks': sum(row['picks'] for row in latest['hero_stats']),
        'hero_wins': sum(row['wins'] for row in latest['hero_stats'])})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=['batch', 'replay'])
    args = parser.parse_args()
    run_id = f"ci-{args.kind}-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    {'batch': batch, 'replay': replay}[args.kind](run_id)


if __name__ == '__main__':
    main()
