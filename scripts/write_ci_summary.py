"""Summarize fresh fixture reports, without claiming a full-dataset or cluster run."""
import argparse
import json
import os
from pathlib import Path
import sys

PROJECT=Path(__file__).resolve().parents[1]
REPORTS={
    'spark-web':['evidence/step2/rule_tests.json','evidence/step3/rule_tests.json',
                 'evidence/step4/feature_tests.json','evidence/step4/training_tests.json',
                 'evidence/step4/inference_tests.json','evidence/step5/api_tests.json',
                 'evidence/step5/display_tests.json','evidence/step5/export_tests.json',
                 'evidence/step6/api_tests.json','evidence/step6/rule_tests.json'],
    'airflow':['evidence/step7/runner_tests.json','evidence/step7/dag_tests.json'],
    'repository':[],
}


def build_summary(suite,job_status,root=PROJECT):
    rows=[]
    for name in REPORTS[suite]:
        path=Path(root)/name
        if not path.exists():
            status='NOT GENERATED'
        else:
            try:
                value=json.loads(path.read_bytes())
                status='PASS' if isinstance(value,dict) and value.get('status')=='PASS' else 'FAIL'
            except (ValueError,OSError,UnicodeError):
                status='UNREADABLE'
        rows.append((name,status))
    complete=all(status=='PASS' for _,status in rows)
    text=[f'## Dota 2 — {suite} checks','',f'Job status before artifact upload: **{job_status}**.','',
          'CI validates source code and synthetic/local fixtures. It does not process the full Zenodo dataset or start the WSL HDFS/Kafka cluster.','']
    if rows:
        text+=['| Fresh fixture report | Result |','|---|---|']
        text+=[f'| `{name}` | {status} |' for name,status in rows]
        text+=['','Download fixture reports and test logs under this run’s Artifacts. Retention: 14 days.','']
    else:
        text+=['Checks: JSON, tracked-file policy and PowerShell syntax. Recorded project evidence is downloadable from the separate publication workflow; local `evidence/` is ignored by Git.','']
    return '\n'.join(text),job_status=='success' and not complete


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite',choices=REPORTS,required=True)
    parser.add_argument('--job-status',choices=['success','failure','cancelled'],required=True)
    args=parser.parse_args()
    text,failed=build_summary(args.suite,args.job_status)
    destination=os.environ.get('GITHUB_STEP_SUMMARY')
    if destination:
        with Path(destination).open('a',encoding='utf-8') as stream:
            stream.write(text+'\n')
    else:
        print(text)
    if failed:
        print('Successful job is missing passing fixture reports.',file=sys.stderr)
    return int(failed)


if __name__=='__main__':
    sys.exit(main())
