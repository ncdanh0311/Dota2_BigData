#!/usr/bin/env python3
"""Operate the isolated Dota HDFS teaching cluster inside Ubuntu WSL."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from orchestration.runtime_config import load_config
RUNTIME = load_config('hdfs')
ROOT = Path(RUNTIME['cluster_root'])
HADOOP = Path(RUNTIME['hadoop_home'])
JAVA = RUNTIME['java_home']
NODES = ['namenode', 'dn1', 'dn2', 'dn3']
FILENAME = 'DB_03_Live_Features_ANALYSIS_v3.csv'
TARGET = '/dota/raw/zenodo_v3/' + FILENAME
URL = 'https://zenodo.org/records/23166182/files/' + FILENAME + '?download=1'
EXPECTED_MD5 = 'f1c2362395ab39d5510a0a008d1cbe26'
EVIDENCE = PROJECT / 'evidence' / 'hdfs'


def env(node='namenode'):
    result = os.environ.copy()
    result.update(HADOOP_HOME=str(HADOOP), JAVA_HOME=JAVA,
                  HADOOP_CONF_DIR=str(ROOT / 'conf' / node),
                  HADOOP_PID_DIR=str(ROOT / 'run' / node),
                  HADOOP_LOG_DIR=str(ROOT / 'logs' / node),
                  HADOOP_IDENT_STRING='dota_' + node)
    return result


def run(*args, node='namenode', check=True):
    command = [str(HADOOP / 'bin' / 'hdfs'), '--config',
               str(ROOT / 'conf' / node), *args]
    if args[:2] == ('--daemon', 'start'):
        command.insert(0, 'nohup')
    result = subprocess.run(command, env=env(node), text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            start_new_session=True)
    if check and result.returncode:
        raise RuntimeError(' '.join(command) + '\n' + result.stdout)
    return result


def save(name, content):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / name).write_text(content, encoding='utf-8')
    (ROOT / 'evidence').mkdir(parents=True, exist_ok=True)
    (ROOT / 'evidence' / name).write_text(content, encoding='utf-8')


def xml(path, properties):
    tree = ET.Element('configuration')
    for key, value in properties.items():
        prop = ET.SubElement(tree, 'property')
        ET.SubElement(prop, 'name').text = key
        ET.SubElement(prop, 'value').text = str(value)
    ET.indent(tree)
    ET.ElementTree(tree).write(path, encoding='utf-8', xml_declaration=True)


def setup():
    marker = ROOT / '.dota-lab'
    if ROOT.exists() and not marker.exists():
        raise RuntimeError(f'{ROOT} already exists without lab marker; refusing to modify it.')
    ROOT.mkdir(parents=True, exist_ok=True)
    marker.touch()
    for node in NODES:
        for parent in ['conf', 'storage', 'run', 'logs']:
            (ROOT / parent / node).mkdir(parents=True, exist_ok=True)
        conf = ROOT / 'conf' / node
        # Copy defaults only once; never modify the system Hadoop configuration.
        if not (conf / 'hadoop-env.sh').exists():
            shutil.copytree(HADOOP / 'etc' / 'hadoop', conf, dirs_exist_ok=True)
        xml(conf / 'core-site.xml', {'fs.defaultFS': 'hdfs://localhost:9000'})
        props = {'dfs.replication': 2, 'dfs.blocksize': 67108864}
        if node == 'namenode':
            props.update({'dfs.namenode.name.dir': (ROOT / 'storage' / node).as_uri(),
                          'dfs.namenode.rpc-address': 'localhost:9000',
                          'dfs.namenode.http-address': '127.0.0.1:9870'})
        else:
            offset = (int(node[-1]) - 1) * 100
            props.update({'dfs.datanode.data.dir': (ROOT / 'storage' / node).as_uri(),
                          'dfs.datanode.address': f'127.0.0.1:{9866 + offset}',
                          'dfs.datanode.http.address': f'127.0.0.1:{9864 + offset}',
                          'dfs.datanode.ipc.address': f'127.0.0.1:{9867 + offset}'})
        xml(conf / 'hdfs-site.xml', props)
        # Own deterministic env file, so previous JVM options cannot override the heap.
        (conf / 'hadoop-env.sh').write_text(
            f'export JAVA_HOME={JAVA}\n'
            f'export HADOOP_HEAPSIZE_MAX={512 if node == "namenode" else 256}\n'
            f'export HADOOP_PID_DIR={ROOT / "run" / node}\n'
            f'export HADOOP_LOG_DIR={ROOT / "logs" / node}\n'
            f'export HADOOP_IDENT_STRING=dota_{node}\n', encoding='utf-8')
    for directory in ['source', 'evidence']:
        (ROOT / directory).mkdir(exist_ok=True)
    print(f'Isolated configuration ready: {ROOT}', flush=True)


def daemon(node, action):
    kind = 'namenode' if node == 'namenode' else 'datanode'
    result = run('--daemon', action, kind, node=node)
    print(f'{node}: {action} {result.stdout.strip()}', flush=True)


def wait_ready():
    for _ in range(90):
        report = run('dfsadmin', '-report', check=False)
        if report.returncode == 0 and 'Live datanodes (3)' in report.stdout:
            safe = run('dfsadmin', '-safemode', 'get', check=False)
            if safe.returncode == 0 and 'Safe mode is OFF' in safe.stdout:
                return
        time.sleep(2)
    raise RuntimeError(f'Cluster not ready; inspect {ROOT}/logs')


def start():
    version = ROOT / 'storage' / 'namenode' / 'current' / 'VERSION'
    if not version.exists():
        if any((ROOT / 'storage' / 'namenode').iterdir()):
            raise RuntimeError('NameNode storage is nonempty without VERSION; refusing format.')
        for port in [9000, 9870, 9866, 9966, 10066, 9864, 9964, 10064, 9867, 9967, 10067]:
            with socket.socket() as sock:
                if sock.connect_ex(('127.0.0.1', port)) == 0:
                    raise RuntimeError(f'Port {port} occupied; refusing initial startup.')
        save('format.txt', run('namenode', '-format', '-nonInteractive', 'dota-course').stdout)
    for node in NODES:
        kind = 'namenode' if node == 'namenode' else 'datanode'
        status = run('--daemon', 'status', kind, node=node, check=False)
        if status.returncode != 0 or 'is stopped' in status.stdout:
            daemon(node, 'start')
    wait_ready()
    print('Ready: 3 live DataNodes; safe mode OFF.', flush=True)


def stop():
    for node in ['dn3', 'dn2', 'dn1', 'namenode']:
        daemon(node, 'stop')


def digest(path, algorithm):
    value = hashlib.new(algorithm)
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def download():
    dest = ROOT / 'source' / FILENAME
    if dest.exists():
        if digest(dest, 'md5') != EXPECTED_MD5:
            raise RuntimeError('Existing source checksum mismatch; refusing overwrite.')
    else:
        part = dest.with_suffix('.csv.part')
        # Bounded parallel HTTP ranges; assemble and verify the published MD5.
        size = 223002035
        count = 16
        chunk_size = (size + count - 1) // count
        chunks = []
        for index in range(count):
            begin = index * chunk_size
            end = min(size - 1, begin + chunk_size - 1)
            chunks.append((ROOT / 'source' / f'{FILENAME}.range-{index:02d}', begin, end))
        def fetch(chunk):
            path, begin, end = chunk
            if not path.exists() or path.stat().st_size != end - begin + 1:
                subprocess.run(['curl', '-fsSL', '--retry', '3', '--connect-timeout', '30',
                                '--max-time', '900', '--range', f'{begin}-{end}',
                                URL, '-o', str(path)], check=True)
            if path.stat().st_size != end - begin + 1:
                raise RuntimeError(f'Incorrect Range size: {path}')
            print(f'Download part {path.name[-2:]} complete', flush=True)
        with ThreadPoolExecutor(max_workers=count) as pool:
            for future in as_completed([pool.submit(fetch, chunk) for chunk in chunks]):
                future.result()
        with part.open('wb') as output:
            for path, _, _ in chunks:
                with path.open('rb') as source:
                    shutil.copyfileobj(source, output)
        if digest(part, 'md5') != EXPECTED_MD5:
            raise RuntimeError('Downloaded MD5 differs from published v3 checksum.')
        part.rename(dest)
    with dest.open(newline='', encoding='utf-8-sig') as file:
        reader = csv.reader(file)
        header = next(reader)
        rows = sum(1 for _ in reader)
    metadata = {'source': 'https://zenodo.org/records/23166182', 'download_url': URL,
                'version': 'v3', 'downloaded_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'filename': FILENAME, 'bytes': dest.stat().st_size,
                'md5': digest(dest, 'md5'), 'sha256': digest(dest, 'sha256'),
                'columns': header, 'column_count': len(header), 'data_rows': rows}
    save('source_manifest.json', json.dumps(metadata, ensure_ascii=False, indent=2))
    reference = PROJECT / 'data' / 'reference'
    reference.mkdir(parents=True, exist_ok=True)
    (reference / 'source_manifest.json').write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(metadata, ensure_ascii=False, indent=2), flush=True)


def ingest():
    run('dfs', '-mkdir', '-p', '/dota/raw/zenodo_v3', '/dota/processed', '/dota/analytics')
    exists = run('dfs', '-test', '-e', TARGET, check=False)
    if exists.returncode == 0:
        print('Target exists; verify instead of overwriting.', flush=True)
        return
    run('dfs', '-put', str(ROOT / 'source' / FILENAME), TARGET)
    run('dfs', '-setrep', '-w', '2', TARGET)
    print(f'Uploaded: {TARGET}', flush=True)


def hdfs_sha():
    command = [str(HADOOP / 'bin' / 'hdfs'), '--config', str(ROOT / 'conf' / 'namenode'),
               'dfs', '-cat', TARGET]
    with subprocess.Popen(command, env=env(), stdout=subprocess.PIPE) as process:
        value = hashlib.sha256()
        for chunk in iter(lambda: process.stdout.read(1024 * 1024), b''):
            value.update(chunk)
        if process.wait() != 0:
            raise RuntimeError('Reading from HDFS failed.')
    return value.hexdigest()


def verify(label='verification'):
    report = run('dfsadmin', '-report').stdout
    listing = run('dfs', '-ls', '-h', '/dota/raw/zenodo_v3').stdout
    usage = run('dfs', '-du', '/dota/raw/zenodo_v3').stdout
    fsck = run('fsck', TARGET, '-files', '-blocks', '-locations').stdout
    save(f'{label}_report.txt', report)
    save(f'{label}_files.txt', listing + '\n' + usage)
    save(f'{label}_fsck.txt', fsck)
    if 'Live datanodes (3)' not in report or 'is HEALTHY' not in fsck:
        raise RuntimeError('Cluster health check failed.')
    replicas = re.findall(r'Live_repl=(\d+)', fsck)
    expected_blocks = ((ROOT / 'source' / FILENAME).stat().st_size + 67108863) // 67108864
    if len(replicas) != expected_blocks or any(value != '2' for value in replicas):
        raise RuntimeError('Unexpected per-block replicas: ' + repr(replicas))
    actual = run('dfs', '-stat', '%r %o %b', TARGET).stdout.strip()
    if not actual.startswith('2 67108864 '):
        raise RuntimeError('Unexpected replication or block size: ' + actual)
    expected = digest(ROOT / 'source' / FILENAME, 'sha256')
    observed = hdfs_sha()
    if observed != expected:
        raise RuntimeError('HDFS/source SHA-256 mismatch.')
    save(f'{label}_checksum.json', json.dumps({'source_sha256': expected,
         'hdfs_sha256': observed, 'match': True, 'file_stat': actual}, indent=2))
    print(fsck, flush=True)
    print('PASS: HDFS checksum equals source.', flush=True)


def failure_test():
    daemon('dn3', 'stop')
    try:
        expected = digest(ROOT / 'source' / FILENAME, 'sha256')
        actual = hdfs_sha()
        if actual != expected:
            raise RuntimeError('Checksum mismatch with DN3 stopped.')
        save('failure_test.json', json.dumps({'stopped_node': 'dn3',
             'sha256': actual, 'read_succeeded': True,
             'note': 'Process failure only; all nodes share the WSL host.'}, indent=2))
        print('PASS: full file read with DN3 stopped.', flush=True)
    finally:
        daemon('dn3', 'start')
        wait_ready()
    run('dfs', '-setrep', '-w', '2', TARGET)
    verify('after_failure')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'start', 'stop', 'download', 'ingest',
                                         'verify', 'restart-test', 'failure-test', 'status'])
    action = parser.parse_args().action
    if action == 'setup': setup()
    elif action == 'start': start()
    elif action == 'stop': stop()
    elif action == 'download': download()
    elif action == 'ingest': ingest()
    elif action == 'verify': verify()
    elif action == 'restart-test':
        stop()
        start()
        verify('after_restart')
    elif action == 'failure-test': failure_test()
    elif action == 'status': print(run('dfsadmin', '-report').stdout)


if __name__ == '__main__':
    main()
