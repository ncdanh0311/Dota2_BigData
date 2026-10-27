"""Bounded Linux resource sampler for the local Airflow demo (no command lines in evidence)."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.runner import write


def sample():
    totals={name:0 for name in ('airflow','spark','hdfs','kafka','web')}
    counts={name:0 for name in totals}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit() or int(entry.name)==os.getpid():
            continue
        try:
            command=(entry/'cmdline').read_bytes().decode(errors='replace').split('\0')
            text=' '.join(command)
            group=('spark' if 'org.apache.spark.deploy.SparkSubmit' in command else
                   'hdfs' if any(x in command for x in ('org.apache.hadoop.hdfs.server.namenode.NameNode','org.apache.hadoop.hdfs.server.datanode.DataNode')) else
                   'kafka' if 'kafka.Kafka' in command else
                   'airflow' if any('/dota-airflow/venv/' in part for part in command) or text.startswith('airflow ') else
                   'web' if '.venv-web/bin/python' in text else None)
            if group:
                pss=next(line for line in (entry/'smaps_rollup').read_text().splitlines() if line.startswith('Pss:'))
                totals[group]+=int(pss.split()[1])
                counts[group]+=1
        except (OSError,StopIteration):
            continue
    memory={line.split(':')[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.split(':')[0] in ('MemTotal','MemAvailable','SwapTotal','SwapFree')}
    return {'at_utc':datetime.now(timezone.utc).isoformat(),'process_pss_kib':totals,'process_counts':counts,**memory}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration',type=int,default=1200)
    parser.add_argument('--interval',type=int,default=10)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.duration<1 or args.interval<1:
        parser.error('duration/interval must be positive')
    rows=[]
    started=time.monotonic()
    while time.monotonic()-started<args.duration:
        rows.append(sample())
        write(args.output,{'source':'Linux /proc; proportional set size, KiB','samples':rows,
            'minimum_available_kib':min(row['MemAvailable'] for row in rows),
            'peak_process_pss_kib':{name:max(row['process_pss_kib'][name] for row in rows) for name in rows[0]['process_pss_kib']}})
        time.sleep(args.interval)
    print('Resource samples saved:',args.output)


if __name__=='__main__':
    main()
