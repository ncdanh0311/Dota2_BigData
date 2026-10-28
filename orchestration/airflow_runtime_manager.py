#!/usr/bin/env python3
"""Isolated local Airflow manager; never resets its database or other services."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from web.snapshot import PROJECT
from orchestration.runtime_config import load_config

CONFIG=load_config('airflow')
ROOT=Path(CONFIG['root'])
VENV=ROOT/'venv'
RUNTIME=ROOT/'runtime'


def environment():
    return {**os.environ, 'AIRFLOW_HOME':str(RUNTIME), 'AIRFLOW__CORE__EXECUTOR':'LocalExecutor',
        'AIRFLOW__CORE__PARALLELISM':'1','AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG':'1',
        'AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG':'1','AIRFLOW__CORE__LOAD_EXAMPLES':'False',
        'AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION':'True','AIRFLOW__CORE__DEFAULT_TIMEZONE':'Asia/Ho_Chi_Minh',
        'AIRFLOW__CORE__DAGS_FOLDER':str(PROJECT/'orchestration/dags'),
        'AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES':'1','AIRFLOW__API__WORKERS':'1',
        'AIRFLOW__API__HOST':CONFIG['host'],'AIRFLOW__API__PORT':str(CONFIG['port']),
        'AIRFLOW__API__BASE_URL':f'http://127.0.0.1:{CONFIG["port"]}',
        'AIRFLOW__CORE__EXECUTION_API_SERVER_URL':f'http://127.0.0.1:{CONFIG["port"]}/execution/',
        'AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS':'False',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN':'sqlite:///'+str(RUNTIME/'airflow.db'),
        'PYTHONPATH':str(PROJECT), 'PATH':str(VENV/'bin')+':'+os.environ.get('PATH','')}


def cli(*args,**kwargs):
    return subprocess.run([str(VENV/'bin/airflow'),*map(str,args)],env=environment(),check=True,**kwargs)


def setup(constraints):
    ROOT.mkdir(parents=True,exist_ok=True)
    RUNTIME.mkdir(exist_ok=True)
    ROOT.chmod(0o700)
    RUNTIME.chmod(0o700)
    if not (VENV/'bin/python').exists():
        subprocess.run([sys.executable,'-m','venv',str(VENV)],check=True)
    # The Ubuntu venv bundled pip predates Python 3.14 and mis-resolves its wheel constraints.
    subprocess.run([str(VENV/'bin/python'),'-m','pip','install','pip==26.2.2'],check=True)
    data=Path(constraints).read_bytes()
    constraint_path=ROOT/f'constraints-{sys.version_info.major}.{sys.version_info.minor}.txt'
    constraint_path.write_bytes(data)
    subprocess.run([str(VENV/'bin/python'),'-m','pip','install','-r',str(PROJECT/'requirements/airflow.txt'),
        '--constraint',str(constraint_path)],check=True)
    subprocess.run([str(VENV/'bin/python'),'-m','pip','check'],check=True)
    cli('db','migrate')
    cli('pools','set',CONFIG['spark_pool'],'1','One Spark job at a time')
    # Own development database only; persist journal mode across component connections.
    import sqlite3
    with sqlite3.connect(RUNTIME/'airflow.db') as db:
        mode=db.execute('PRAGMA journal_mode=WAL').fetchone()[0]
    evidence=PROJECT/'evidence/step7'
    evidence.mkdir(parents=True,exist_ok=True)
    (evidence/'installation.json').write_text(json.dumps({'airflow':CONFIG['version'],'python':sys.version.split()[0],
        'sqlite':sqlite3.sqlite_version,'journal_mode':mode,'constraints_sha256':hashlib.sha256(data).hexdigest(),
        'root':str(ROOT),'port':CONFIG['port'],'parallelism':1,'status':'PASS'},indent=2)+'\n')
    print('Airflow installed in isolated venv; database/history preserved.')


def owned_pid():
    file=ROOT/'airflow.pid'
    if not file.exists():
        return None
    pid=int(file.read_text())
    try:
        parts=Path(f'/proc/{pid}/cmdline').read_bytes().decode().split('\0')
    except FileNotFoundError:
        return None
    if str(VENV/'bin/airflow') not in parts or 'standalone' not in parts:
        raise RuntimeError('Airflow PID belongs to another command; refusing to signal it.')
    return pid


def start():
    if not (RUNTIME/'airflow.db').exists():
        raise ValueError('Run commands/install-airflow.ps1 first.')
    ROOT.chmod(0o700)
    RUNTIME.chmod(0o700)
    if owned_pid():
        print('Airflow instance already running.')
        return
    with socket.socket() as probe:
        if probe.connect_ex((CONFIG['host'],CONFIG['port']))==0:
            raise RuntimeError('Airflow port is occupied; no existing service was stopped.')
    with (ROOT/'launcher.log').open('ab') as output:
        process=subprocess.Popen([str(VENV/'bin/airflow'),'standalone'],env=environment(),
            cwd=PROJECT,stdin=subprocess.DEVNULL,stdout=output,stderr=output,start_new_session=True)
    (ROOT/'airflow.pid').write_text(str(process.pid))
    for _ in range(90):
        if process.poll() is not None:
            raise RuntimeError('Airflow exited. Check its private launcher.log.')
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{CONFIG["port"]}/api/v2/monitor/health',timeout=2) as response:
                health=json.load(response)
            if health.get('scheduler',{}).get('status')=='healthy':
                print(f'Airflow ready: http://localhost:{CONFIG["port"]}')
                print('Login credentials are in '+str(RUNTIME/'simple_auth_manager_passwords.json.generated'))
                return
        except (OSError,ValueError):
            pass
        time.sleep(2)
    raise RuntimeError('Airflow readiness timeout; logs retained, no reset performed.')


def stop():
    pid=owned_pid()
    if not pid:
        print('Airflow is stopped.')
        return
    import sqlite3
    with sqlite3.connect(RUNTIME/'airflow.db') as db:
        active=db.execute("SELECT count(*) FROM task_instance WHERE state IN ('running','queued')").fetchone()[0]
    if active:
        raise RuntimeError('Airflow has active tasks; wait for the DAG or stop it before shutdown.')
    os.killpg(pid,signal.SIGINT)
    for _ in range(120):
        if not process_group_running(pid):
            (ROOT/'airflow.pid').unlink(missing_ok=True)
            print('Airflow stopped; database and history retained.')
            return
        time.sleep(.5)
    raise RuntimeError('Airflow shutdown is not complete; no unrelated process was signaled.')


def process_group_running(group_id, proc=Path('/proc')):
    """A zombie PID is already stopped; also wait for live children in our session."""
    if group_id <= 0:
        raise ValueError('Expected a positive owned process-group ID.')
    for stat in proc.glob('[0-9]*/stat'):
        try:
            # The comm field can contain spaces/parentheses; fields after it start state, ppid, pgrp.
            fields=stat.read_text().rsplit(')',1)[1].split()
            if int(fields[2])==group_id and fields[0] not in {'Z','X'}:
                return True
        except (OSError,ValueError,IndexError):
            continue  # Process exited between enumeration and reading /proc.
    return False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['setup','start','stop','check'])
    parser.add_argument('--constraints')
    args=parser.parse_args()
    if args.action=='setup':
        setup(args.constraints)
    elif args.action=='start':
        start()
    elif args.action=='stop':
        stop()
    else:
        cli('version')
        cli('db','check')
        cli('dags','list-import-errors')


if __name__=='__main__':
    main()
