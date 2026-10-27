"""Verify completed real Step 7 demo runs through Airflow API and local receipts."""
import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.control import api, token
from orchestration.runner import Runner, identity, read, write
from web.snapshot import PROJECT, load_snapshot


def verify_run(access,dag_id,run_id,expected):
    from urllib.parse import quote
    endpoint='dags/'+dag_id+'/dagRuns/'+quote(run_id,safe='')
    run=api(access,endpoint)
    tasks=api(access,endpoint+'/taskInstances')['task_instances']
    assert run['state']==expected,(run_id,run['state'])
    execution_id=identity(dag_id,run_id)
    directory=PROJECT/'artifacts/airflow'/dag_id/execution_id
    report=read(directory/'report.json')
    assert report['status']==('PASS' if expected=='success' else 'FAILED')
    assert (directory/'_COMPLETED').exists()==(expected=='success')
    final=next(row for row in tasks if row['task_id']=='verify_and_report')
    assert final['state']==expected
    if expected=='success':
        assert all(row['state']=='success' for row in tasks)
        runner=Runner({'dag_id':dag_id,'run_id':run_id,'try_number':1},report['params'],
                      'batch' if dag_id=='dota_batch_pipeline' else 'replay')
        final_receipts=sorted(directory.glob('attempts/*verify_and_report*/receipt.json'),key=lambda path:path.stat().st_mtime)
        assert final_receipts
        final_receipt=read(final_receipts[-1])
        visited=set()
        def visit(stage,ref):
            if ref['path'] in visited:
                return
            visited.add(ref['path'])
            runner.receipt(ref,stage)
            receipt=read(ref['path'])
            for name,source in receipt['upstream_receipts'].items():
                visit(name,source)
        for stage,ref in final_receipt['upstream_receipts'].items():
            visit(stage,ref)
    return report,{'dag_id':dag_id,'run_id':run_id,'state':run['state'],'duration_seconds':run['duration'],
        'execution_id':execution_id,'completion_marker':(directory/'_COMPLETED').exists(),
        'tasks':[{key:row[key] for key in ('task_id','state','try_number','duration')} for row in tasks]}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-run-id',required=True)
    parser.add_argument('--replay-run-id',required=True)
    parser.add_argument('--failure-run-id',required=True)
    args=parser.parse_args()
    access=token()
    batch,batch_state=verify_run(access,'dota_batch_pipeline',args.batch_run_id,'success')
    replay,replay_state=verify_run(access,'dota_replay_demo',args.replay_run_id,'success')
    failure,failure_state=verify_run(access,'dota_batch_pipeline',args.failure_run_id,'failed')
    chain=batch['summary']['source_chain']
    silver=read(chain['normalize_silver']['report_path'])
    gold=read(chain['analyze_gold']['report_path'])
    model=read(chain['train_lr']['manifest_path'])
    snapshot=load_snapshot(chain['export_snapshot']['path'])
    assert silver['status']=='PASS' and silver['parquet_readback_verified']
    assert gold['status']=='PASS' and gold['gold_readback_verified'] and model['status']=='PASS'
    assert silver['metrics']['valid_matches']==snapshot['dashboard']['total_matches']==90530
    assert gold['input_fingerprint_sha256']==model['input_fingerprint_sha256']==snapshot['manifest']['input_fingerprint_sha256']
    if replay['params']['demo_fail_once']:
        producer=next(row for row in replay_state['tasks'] if row['task_id']=='produce_events')
        assert producer['try_number']==2
        assert replay['summary']['total_records']==2*replay['params']['limit']
        assert replay['summary']['duplicate_records']==replay['params']['limit']
    assert replay['summary']['unique_matches']==replay['params']['limit']
    assert replay['summary']['quarantined_records']==0
    directory=PROJECT/'evidence/step7'
    logic={str(path.relative_to(PROJECT)):hashlib.sha256(path.read_bytes()).hexdigest()
           for path in sorted((PROJECT/'orchestration').rglob('*.py'))}
    write(directory/'acceptance.json',{'status':'PASS','source':'real WSL Airflow + HDFS + Kafka',
        'verified_at_utc':datetime.now(timezone.utc).isoformat(),
        'batch':batch_state,'replay':replay_state,'expected_preflight_failure':failure_state,
        'matches':90530,'gold_model_silver_fingerprint':gold['input_fingerprint_sha256'],
        'snapshot_id':snapshot['manifest']['snapshot_id'],'replay_result':replay['summary'],
        'current_orchestration_sha256':logic,
        'checks':['successful Airflow states','receipt DAG/checksums','completion markers',
                  'failure-sensitive finalizer','Silver/Gold readback','source fingerprint parity',
                  'actual producer retry and duplicate counts']})
    print('PASS: real batch, Kafka retry and expected preflight failure verified.')


if __name__=='__main__':
    main()
