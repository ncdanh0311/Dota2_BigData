"""Install verified Apache binaries into the disposable GitHub runner directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

PROJECT = Path(__file__).resolve().parents[1]
HADOOP_SHA512 = '04ab94496cc00c8b7a28d03f6308eff8d2a4e7f37a9da5e8e086e4d6fc990e7a94d661908f6a6136039536efb362614b8aecdef185b5fb8ed588f0b152c7aa16'


def fetch(url, target, fallback=None):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    for source in [url]+([fallback] if fallback else []):
        result = subprocess.run(['curl', '--fail', '--location', '--retry', '1', '--retry-all-errors',
            '--connect-timeout', '20', '--max-time', '600', '--speed-limit', '1024', '--speed-time', '45',
            '--output', str(target), source])
        if result.returncode == 0:
            return target
    raise RuntimeError('Official download failed after bounded retries and mirror fallback.')


def digest(path, algorithm='sha256'):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


def configure(root, java_home, python, spark_submit):
    root = Path(root).resolve()
    return {
        'hdfs': {'cluster_root': str(root/'hdfs'), 'hadoop_home': str(root/'hadoop-3.5.0'),
                 'java_home': java_home, 'spark_python': python, 'spark_submit': spark_submit,
                 'spark_master': 'local[2]', 'spark_driver_memory': '1g'},
        'kafka': {'version': '4.1.2', 'download_base': 'https://archive.apache.org/dist/kafka',
                  'root': str(root/'kafka'), 'bootstrap_servers': '127.0.0.1:9092',
                  'controller_port': 9093, 'partitions': 2, 'retention_hours': 168,
                  'connector': 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0'},
        'airflow': {'version': '3.3.2', 'root': str(root/'airflow'), 'host': '127.0.0.1',
                    'port': 8080, 'parallelism': 1, 'spark_pool': 'dota_spark'},
    }


def bootstrap():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('This installer is restricted to disposable GitHub Actions runners.')
    root = Path(os.environ['RUNNER_TEMP'])/'dota-integration'
    root.mkdir(exist_ok=False)
    configs = configure(root, os.environ['JAVA_HOME'], sys.executable,
                        str(Path(sys.executable).parent/'spark-submit'))
    with Path(os.environ['GITHUB_ENV']).open('a', encoding='utf-8') as env:
        for name, value in configs.items():
            path = root/f'{name}.json'
            path.write_text(json.dumps(value, indent=2))
            env.write(f'DOTA_{name.upper()}_CONFIG={path}\n')
        env.write(f'DOTA_STREAMING_PYTHON={sys.executable}\n')
        env.write(f'DOTA_CI_ROOT={root}\nPYTHONPATH={PROJECT}\n')
    archive = Path.home()/'.cache/dota-ci/hadoop-3.5.0.tar.gz'
    if not archive.exists():
        print('::notice title=Runtime::Downloading Hadoop from the official Apache CDN', flush=True)
        fetch('https://dlcdn.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz', archive,
              'https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz')
    if digest(archive, 'sha512') != HADOOP_SHA512:
        raise ValueError('Official Hadoop SHA-512 mismatch.')
    with tarfile.open(archive) as bundle:
        bundle.extractall(root, filter='data')
    print('::notice title=Runtime::Hadoop SHA-512 verified and extracted', flush=True)
    print('Verified Hadoop installed; configs isolated under RUNNER_TEMP. WSL configs unchanged.')


def source():
    from orchestration.runtime_config import load_config
    root = Path(load_config('hdfs')['cluster_root'])/'source'
    manifest = json.loads((PROJECT/'data/reference/source_manifest.json').read_bytes())
    path = fetch(manifest['download_url'], root/manifest['filename'])
    if path.stat().st_size != manifest['bytes'] or digest(path) != manifest['sha256']:
        raise ValueError('Zenodo source size/SHA-256 mismatch; refusing HDFS ingestion.')
    directory = PROJECT/'artifacts/integration/reports'
    directory.mkdir(parents=True, exist_ok=True)
    (directory/'source.json').write_text(json.dumps({**manifest, 'verified_on_runner': True}, indent=2))


def airflow():
    from orchestration.airflow_runtime_manager import setup, start
    constraints = fetch('https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt',
                        Path(os.environ['DOTA_CI_ROOT'])/'constraints.txt')
    setup(constraints)
    start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['bootstrap', 'source', 'airflow'])
    args = parser.parse_args()
    {'bootstrap': bootstrap, 'source': source, 'airflow': airflow}[args.action]()


if __name__ == '__main__':
    main()
