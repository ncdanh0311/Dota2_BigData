#!/usr/bin/env python3
"""Local Airflow REST control for verification; never outputs credentials or tokens."""
import argparse
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.airflow_lab import CONFIG,RUNTIME
from orchestration.runner import identity,write
from web.snapshot import PROJECT


def token():
    credentials=json.loads((RUNTIME/'simple_auth_manager_passwords.json.generated').read_bytes())
    password=credentials['admin']
    request=urllib.request.Request(f'http://127.0.0.1:{CONFIG["port"]}/auth/token',
        data=json.dumps({'username':'admin','password':password}).encode(),
        headers={'Content-Type':'application/json'},method='POST')
    try:
        with urllib.request.urlopen(request,timeout=15) as response:
            return json.load(response)['access_token']
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'Local Airflow authentication failed (HTTP {error.code}); inspect the private credential file.') from None


def api(access,path,method='GET',payload=None):
    request=urllib.request.Request(f'http://127.0.0.1:{CONFIG["port"]}/api/v2/'+path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={'Authorization':'Bearer '+access,'Content-Type':'application/json'},method=method)
    try:
        with urllib.request.urlopen(request,timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'Airflow API {method} {path}: HTTP {error.code}: '+error.read().decode()[:1200]) from None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['trigger','status','wait','tasks'])
    parser.add_argument('--dag-id',choices=['dota_batch_pipeline','dota_replay_demo'],required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--conf',default='{}')
    parser.add_argument('--conf-file',type=Path,help='JSON file, convenient when calling from PowerShell')
    parser.add_argument('--timeout',type=int,default=2400)
    parser.add_argument('--expect',choices=['success','failed'],default='success')
    args=parser.parse_args()
    access=token()
    dag='dags/'+args.dag_id
    run=dag+'/dagRuns/'+urllib.parse.quote(args.run_id,safe='')
    if args.action=='trigger':
        api(access,dag,'PATCH',{'is_paused':False})
        conf=json.loads(args.conf_file.read_text(encoding='utf-8-sig')) if args.conf_file else json.loads(args.conf)
        result=api(access,dag+'/dagRuns','POST',{'dag_run_id':args.run_id,'logical_date':None,'conf':conf})
    elif args.action=='tasks':
        entries=api(access,run+'/taskInstances')
        result={'tasks':[{key:row.get(key) for key in ('task_id','state','try_number','duration')}
                         for row in entries['task_instances']]}
    elif args.action=='status':
        result=api(access,run)
    else:
        started=time.monotonic()
        previous=None
        while time.monotonic()-started<args.timeout:
            result=api(access,run)
            state=result['state']
            if state!=previous:
                print(f'{args.run_id}: {state}',flush=True)
                previous=state
            if state in ('success','failed'):
                break
            time.sleep(10)
        else:
            raise TimeoutError('DAG did not finish within the bounded verification wait.')
        tasks=api(access,run+'/taskInstances')
        execution_id=identity(args.dag_id,args.run_id)
        evidence=PROJECT/'evidence/step7'/execution_id
        write(evidence/'airflow_run.json',result)
        write(evidence/'airflow_tasks.json',tasks)
        if result['state']!=args.expect:
            raise RuntimeError(f'DAG ended {result["state"]}, expected {args.expect}. Evidence retained.')
    print(json.dumps(result,ensure_ascii=False,default=str),flush=True)


if __name__=='__main__':
    main()
