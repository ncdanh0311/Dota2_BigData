#!/usr/bin/env python3
"""Operate only the isolated Kafka coursework cluster; start never formats storage."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from web.snapshot import PROJECT

CONFIG = json.loads((PROJECT/'configs/kafka-wsl.json').read_bytes())
RUNTIME = json.loads((PROJECT/'configs/hdfs-wsl.json').read_bytes())
ROOT = Path(CONFIG['root'])
HOME = ROOT/f'kafka_2.13-{CONFIG["version"]}'
SERVER_CONFIG = ROOT/'server.properties'


def environment():
    return {**os.environ, 'JAVA_HOME': RUNTIME['java_home'],
            'KAFKA_HEAP_OPTS': '-Xms256m -Xmx512m', 'LOG_DIR': str(ROOT/'logs')}


def command(name, *args, **kwargs):
    return subprocess.run([str(HOME/'bin'/name), *map(str,args)], env=environment(), check=True, **kwargs)


def setup(archive_path=None, checksum_path=None):
    ROOT.mkdir(parents=True, exist_ok=True)
    version = CONFIG['version']
    filename = f'kafka_2.13-{version}.tgz'
    url = f'{CONFIG["download_base"]}/{version}/{filename}'
    expected_text = Path(checksum_path).read_text() if checksum_path else urllib.request.urlopen(url+'.sha512', timeout=30).read().decode()
    expected = ''.join(re.findall(r'\b[0-9A-Fa-f]{8}\b', expected_text)).lower()
    if len(expected) != 128:
        raise ValueError('Invalid official Kafka SHA-512 document.')
    archive = Path(archive_path) if archive_path else ROOT/filename
    if not archive.exists():
        partial = archive.with_suffix('.part')
        with urllib.request.urlopen(url, timeout=60) as response, partial.open('wb') as output:
            while chunk := response.read(1024*1024):
                output.write(chunk)
        partial.rename(archive)
    actual = hashlib.file_digest(archive.open('rb'), 'sha512').hexdigest()
    if actual != expected:
        raise ValueError('Kafka archive checksum mismatch; existing file preserved.')
    if not HOME.exists():
        with tarfile.open(archive) as bundle:
            bundle.extractall(ROOT, filter='data')
    properties = '\n'.join([
        'process.roles=broker,controller', 'node.id=1',
        'controller.quorum.bootstrap.servers=127.0.0.1:9093',
        'listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093',
        'advertised.listeners=PLAINTEXT://127.0.0.1:9092',
        'controller.listener.names=CONTROLLER',
        'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'inter.broker.listener.name=PLAINTEXT', f'log.dirs={ROOT}/data',
        'num.partitions=2', 'num.network.threads=2', 'num.io.threads=2',
        'offsets.topic.replication.factor=1', 'transaction.state.log.replication.factor=1',
        'transaction.state.log.min.isr=1', 'group.initial.rebalance.delay.ms=0',
        'auto.create.topics.enable=false', 'log.retention.hours=168',
        'log.segment.bytes=33554432', 'log.retention.check.interval.ms=60000', ''])
    if SERVER_CONFIG.exists() and SERVER_CONFIG.read_text() != properties:
        raise ValueError('Existing Kafka configuration differs; preserved for review.')
    SERVER_CONFIG.write_text(properties)
    if not (ROOT/'data/meta.properties').exists():
        cluster_file = ROOT/'cluster.id'
        if cluster_file.exists():
            raise ValueError('Cluster ID exists but storage metadata is absent; refusing to reformat.')
        cluster_id = command('kafka-storage.sh', 'random-uuid', capture_output=True, text=True).stdout.strip()
        cluster_file.write_text(cluster_id)
        command('kafka-storage.sh', 'format', '--standalone', '-t', cluster_id, '-c', SERVER_CONFIG)
    print(json.dumps({'status':'READY','version':version,'sha512':actual,'root':str(ROOT)}))


def owned_pid():
    pid_file = ROOT/'kafka.pid'
    if not pid_file.exists():
        return None
    pid = int(pid_file.read_text())
    try:
        arguments = Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ')
    except FileNotFoundError:
        return None
    if b'kafka.Kafka' not in arguments or str(SERVER_CONFIG).encode() not in arguments:
        raise RuntimeError('PID belongs to another process; refusing to signal it.')
    return pid


def start():
    if not (ROOT/'data/meta.properties').exists():
        raise ValueError('Run install-kafka.ps1 first; start never formats Kafka.')
    if owned_pid():
        print('Kafka already running.')
        return
    for port in [9092, 9093]:
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', port)) == 0:
                raise RuntimeError(f'Port {port} is already occupied.')
    (ROOT/'logs').mkdir(exist_ok=True)
    with (ROOT/'logs/launcher.log').open('ab') as output:
        process = subprocess.Popen([str(HOME/'bin/kafka-server-start.sh'), str(SERVER_CONFIG)],
            env=environment(), stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True)
    (ROOT/'kafka.pid').write_text(str(process.pid))
    for _ in range(40):
        try:
            command('kafka-topics.sh', '--bootstrap-server', CONFIG['bootstrap_servers'], '--list',
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=8)
            print('Kafka ready: '+CONFIG['bootstrap_servers'])
            return
        except (subprocess.SubprocessError, OSError):
            if process.poll() is not None:
                raise RuntimeError('Kafka exited; check its launcher.log.')
            time.sleep(1)
    raise RuntimeError('Kafka readiness timed out; process/logs retained.')


def stop():
    pid = owned_pid()
    if pid:
        os.kill(pid, signal.SIGTERM)
        for _ in range(60):
            if not Path(f'/proc/{pid}').exists():
                break
            time.sleep(.5)
        else:
            raise RuntimeError('Kafka did not stop within 30 seconds; no force kill performed.')
    (ROOT/'kafka.pid').unlink(missing_ok=True)
    print('Kafka stopped; data and cluster ID preserved.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup','start','stop'])
    parser.add_argument('--archive-path')
    parser.add_argument('--checksum-path')
    args = parser.parse_args()
    try:
        if args.action == 'setup':
            setup(args.archive_path,args.checksum_path)
        else:
            {'start':start,'stop':stop}[args.action]()
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
