"""Build downloadable evidence with explicit provenance; stdlib only."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

from write_ci_summary import REPORTS

PROJECT=Path(__file__).resolve().parents[1]
ARCHIVE_TYPES={'.json','.md','.txt','.png','.jpg','.jpeg','.svg','.csv'}


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_files(source):
    source=Path(source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f'Evidence source directory missing: {source}')
    for path in sorted(source.rglob('*')):
        if path.is_symlink():
            raise ValueError(f'Evidence symlink is not allowed: {path}')
        if path.is_file():
            if not path.resolve().is_relative_to(source):
                raise ValueError('Evidence path escapes source.')
            if path.name.startswith('.env') or 'password' in path.name.lower() or path.suffix in {'.db','.sqlite'}:
                raise ValueError('Runtime/credential file in evidence source.')
            yield path


def package(output,*,kind,suite=None,project=PROJECT,source=None,logs=None,revision=None,job_status='success'):
    output,project=Path(output).resolve(),Path(project).resolve()
    records=[]
    missing=[]
    statuses=[]
    candidates=[]
    if kind=='ci_fixture':
        if suite not in REPORTS:
            raise ValueError('CI suite is required.')
        for relative in REPORTS[suite]:
            path=project/relative
            if not path.is_file():
                missing.append(relative)
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(project):
                raise ValueError('Report path escapes the project.')
            try:
                report=json.loads(path.read_bytes())
                statuses.append(report.get('status') if isinstance(report,dict) else 'INVALID')
            except (ValueError,UnicodeError):
                statuses.append('UNREADABLE')
            candidates.append((path,Path(relative),'fixture_report'))
    elif kind=='recorded_project':
        source=Path(source).resolve()
        if not revision:
            raise ValueError('Recorded evidence requires its source revision.')
        for path in safe_files(source):
            if path.suffix.lower() in ARCHIVE_TYPES:
                candidates.append((path,Path('evidence')/path.relative_to(source),'recorded_evidence'))
        if not candidates:
            raise ValueError('Recorded evidence is empty.')
    else:
        raise ValueError('Unknown evidence kind.')
    if logs and Path(logs).is_dir():
        logs=Path(logs).resolve()
        candidates.extend((path,Path('logs')/path.relative_to(logs),'ci_log') for path in safe_files(logs))
    output.mkdir(parents=True,exist_ok=False)
    for path,relative,role in candidates:
        if path.stat().st_size>10*1024*1024:
            raise ValueError(f'Evidence file exceeds 10 MiB: {path.name}')
        target=output/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,target)
        records.append({'path':relative.as_posix(),'role':role,'bytes':target.stat().st_size,'sha256':checksum(target)})
    verified=kind=='ci_fixture' and job_status=='success' and not missing and all(status=='PASS' for status in statuses)
    manifest={'schema_version':1,'source_kind':kind,'suite':suite,
        'source_revision':revision or os.environ.get('GITHUB_SHA','local-uncommitted'),
        'workflow_revision':os.environ.get('GITHUB_SHA'),'workflow_run_id':os.environ.get('GITHUB_RUN_ID'),
        'workflow_run_attempt':os.environ.get('GITHUB_RUN_ATTEMPT'),
        'packaged_at_utc':datetime.now(timezone.utc).isoformat(),
        'status':'ARCHIVED' if kind=='recorded_project' else 'PASS' if verified else 'INCOMPLETE_OR_FAILED',
        'missing_reports':missing,'files':records}
    (output/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    scope=('Bản lưu bằng chứng đồ án từ Git revision ghi bên dưới. Gồm các báo cáo WSL, kiểm thử và ảnh đã ghi nhận; lượt Actions này chỉ đóng gói, không chạy lại HDFS/Kafka/ML.'
           if kind=='recorded_project' else
           'Bằng chứng fixture của lượt CI này. Không phải kết quả chạy toàn bộ dataset Zenodo hoặc cụm HDFS/Kafka thật.')
    rows=['# Gói bằng chứng Dota 2','',scope,'',f'Source revision: `{manifest["source_revision"]}`',
          f'Status: **{manifest["status"]}**',f'Files: **{len(records)}**','',
          'Đọc các file trong `evidence/`; manifest ghi SHA-256 và kích thước từng file. Log CI nằm trong `logs/` nếu có.','']
    if missing:
        rows+=['Báo cáo chưa được tạo:']+['- `'+name+'`' for name in missing]+['']
    (output/'README.md').write_text('\n'.join(rows),encoding='utf-8')
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind',choices=['ci_fixture','recorded_project'],required=True)
    parser.add_argument('--suite',choices=REPORTS)
    parser.add_argument('--source',type=Path)
    parser.add_argument('--logs',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--source-revision')
    parser.add_argument('--job-status',default='success',choices=['success','failure','cancelled'])
    args=parser.parse_args()
    if args.kind=='recorded_project' and args.source is None:
        parser.error('--source is required for recorded evidence')
    manifest=package(args.output,kind=args.kind,suite=args.suite,source=args.source,logs=args.logs,
                     revision=args.source_revision,job_status=args.job_status)
    print(json.dumps({key:manifest[key] for key in ('source_kind','status','missing_reports')},ensure_ascii=False))
    summary=os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with Path(summary).open('a',encoding='utf-8') as stream:
            stream.write(f'\n### Gói tải về\n\nLoại: `{args.kind}` · Trạng thái: **{manifest["status"]}** · {len(manifest["files"])} file.\n\nTải gói ở mục **Artifacts** của lượt chạy này; xem README và manifest trong ZIP.\n')
    return int(args.kind=='ci_fixture' and args.job_status=='success' and manifest['status']!='PASS')


if __name__=='__main__':
    sys.exit(main())
