"""Vietnamese Actions summary, diagnostic collection and checksummed artifact indexes."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil

PROJECT = Path(__file__).resolve().parents[1]
OUT = PROJECT/'artifacts/integration'


def read(path):
    return json.loads(Path(path).read_bytes())


def redact(text, secrets):
    for value in secrets:
        if value:
            text = text.replace(value, '[REDACTED]')
    return text


def collect():
    from orchestration.runtime_config import load_config
    root = Path(os.environ['DOTA_CI_ROOT']).resolve()
    config = load_config('airflow')
    runtime = Path(config['root'])/'runtime'
    password_file = runtime/'simple_auth_manager_passwords.json.generated'
    secrets = list(read(password_file).values()) if password_file.exists() else []
    target = OUT/'evidence'
    target.mkdir(parents=True, exist_ok=True)
    sources = [(PROJECT/'evidence', target/'reports'),
               (PROJECT/'artifacts/airflow', target/'airflow-attempts'),
               (root/'hdfs/logs', target/'hdfs-logs'),
               (root/'kafka/logs', target/'kafka-logs'),
               (runtime/'logs', target/'airflow-task-logs')]
    for source, destination in sources:
        if not source.exists():
            continue
        for path in source.rglob('*'):
            if path.is_symlink() or not path.is_file() or path.suffix not in {'.log', '.json', '.jsonl', '.md', '.txt'}:
                continue
            if path.stat().st_size > 20*1024*1024:
                continue
            if 'password' in path.name.lower() or 'config' in path.name.lower():
                continue
            output = destination/path.relative_to(source)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(redact(path.read_text(errors='replace'), secrets), encoding='utf-8')
    launcher = root/'airflow/launcher.log'
    if launcher.exists():
        (target/'airflow-launcher.log').write_text(redact(launcher.read_text(errors='replace'), secrets), encoding='utf-8')
    if (OUT/'logs').exists():
        shutil.copytree(OUT/'logs', target/'runner-logs', dirs_exist_ok=True)
    if (OUT/'screenshots').exists():
        shutil.copytree(OUT/'screenshots', target/'screenshots', dirs_exist_ok=True)


def index_artifacts(kind, status):
    for group in ['evidence', 'reports', 'silver', 'streaming']:
        root = OUT/group
        if not root.exists():
            continue
        files = []
        for path in sorted(root.rglob('*')):
            if path.is_symlink():
                raise ValueError('Artifact symlinks are not supported.')
            if path.is_file() and path.name not in {'artifact-manifest.json', 'ARTIFACT_README.md'}:
                with path.open('rb') as stream:
                    checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
                files.append({'path': path.relative_to(root).as_posix(), 'bytes': path.stat().st_size, 'sha256': checksum})
        manifest = {'source_kind': 'github_integration', 'pipeline': kind, 'group': group,
            'status': status, 'github_run_id': os.environ.get('GITHUB_RUN_ID'),
            'github_run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'), 'git_commit': os.environ.get('GITHUB_SHA'),
            'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'files': files}
        (root/'artifact-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        (root/'ARTIFACT_README.md').write_text(
            f'# Dota 2 — {kind} / {group}\n\nTrạng thái job trước upload: **{status}**.\n\n'
            'Dữ liệu và log được tạo trong lượt GitHub Actions này; không lấy báo cáo WSL cũ. '
            'Đọc artifact-manifest.json để kiểm tra revision, nguồn và SHA-256 từng file.\n\n'
            'Nếu job lỗi, các file còn lại chỉ phục vụ chẩn đoán; không coi run là hoàn tất.\n', encoding='utf-8')


def verify_artifact(root):
    root = Path(root).resolve()
    manifest = read(root/'artifact-manifest.json')
    if (manifest['source_kind'] != 'github_integration' or manifest['status'] != 'success'
            or str(manifest['github_run_id']) != os.environ['GITHUB_RUN_ID']):
        raise ValueError('Artifact source/run/status mismatch.')
    for record in manifest['files']:
        path = (root/record['path']).resolve()
        if not path.is_relative_to(root) or not path.is_file() or path.stat().st_size != record['bytes']:
            raise ValueError('Artifact file missing or invalid.')
        with path.open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != record['sha256']:
                raise ValueError('Artifact SHA-256 mismatch.')
    return manifest


def task_table(state):
    lines = ['| Task Airflow | Trạng thái | Lần thử | Giây |', '|---|---|---:|---:|']
    for row in state['tasks']:
        duration = row.get('duration')
        lines.append(f"| `{row['task_id']}` | {row['state']} | {row['try_number']} | {duration:.2f} |" if duration is not None
                     else f"| `{row['task_id']}` | {row['state']} | {row['try_number']} | — |")
    return lines


def summary(kind, status, root=OUT):
    root = Path(root)
    title = 'HDFS → Silver → Gold → Spark ML → Website' if kind == 'batch' else 'Silver → Kafka → Airflow retry → Structured Streaming'
    lines = [f'## Kết quả pipeline Dota 2 — {kind}', '', f'**{title}**', '',
        f"Commit: `{os.environ.get('GITHUB_SHA', 'local')}` · Run: `{os.environ.get('GITHUB_RUN_ID', 'local')}` · Trạng thái job: **{status}**", '',
        'Nguồn: `github_integration` — dịch vụ và pipeline chạy trên GitHub runner; các nút HDFS cùng một máy. '
        'Bảng dưới đọc từ output lượt chạy này, không phải báo cáo WSL được đóng gói lại.', '']
    path = root/'reports'/f'{kind}.json'
    failed = status == 'success' and not path.exists()
    if not path.exists():
        lines += ['**Chưa có báo cáo nghiệm thu.** Xem log trong Artifacts để xác định bước bị lỗi.', '']
    else:
        data = read(path)
        verified = data.get('status') == 'PASS' and data.get('source_kind') == 'github_integration'
        failed = failed or not verified
        if not verified:
            lines += ['**Báo cáo chưa đạt nghiệm thu; không xác nhận hoàn tất.**', '']
        elif kind == 'batch':
            gold = data['gold']
            lines += ['### Kiểm chứng dữ liệu', '', '| Chỉ tiêu | Kết quả |', '|---|---:|',
                f"| Snapshot CSV nguồn | {data['raw_rows']:,} |", f"| Trận Silver | {data['matches']:,} |",
                f"| Radiant thắng | {gold['overview']['radiant_wins']:,} |",
                f"| Dire thắng | {gold['overview']['dire_wins']:,} |"]
            lines += [f'| Dòng {name} | {count:,} |' for name, count in gold['row_counts'].items()]
            lines += ['', '### Mô hình trên test', '',
                '| Mô hình | Accuracy | Precision Radiant | Recall Radiant | F1 Radiant | ROC-AUC | Log loss |',
                '|---|---:|---:|---:|---:|---:|---:|']
            for name, item in data['model']['test'].items():
                values = [f"{item[k]*100:.2f}%" for k in ['accuracy', 'precision_radiant', 'recall_radiant', 'f1_radiant']]
                lines.append(f"| {name} | "+' | '.join(values)+f" | {item['roc_auc']:.4f} | {item['log_loss']:.6f} |")
            lines += ['', f"Snapshot: `{data['snapshot_id']}` · Fingerprint: `{data['fingerprint']}`", '',
                'Đã kiểm tra run preflight lỗi kết thúc `failed` và không có marker hoàn tất. '
                'Model chọn theo validation log loss; test không tham gia chọn tham số. '
                'Dữ liệu lịch sử chưa đại diện meta hiện tại. Ảnh hero tải lỗi dùng placeholder local.', '']
        else:
            item = data['summary']
            lines += ['### Kết quả phát lại và chống đếm trùng', '',
                '| Record nhận | Trận duy nhất | Record trùng | Cách ly | Tổng picks | Tổng hero wins |',
                '|---:|---:|---:|---:|---:|---:|',
                f"| {item['total_records']:,} | {item['unique_matches']:,} | {item['duplicate_records']:,} | {item['quarantined_records']:,} | {data['hero_picks']:,} | {data['hero_wins']:,} |", '',
                'Producer gửi đủ rồi lỗi có chủ đích ở lần thử đầu; Airflow retry phát lại cùng catalog. '
                'Đối chiếu bằng phép tổng hợp Python độc lập và giữ checkpoint HDFS trong artifact. '
                'Đây là replay trận lịch sử, không phải nguồn live API.', '']
        if verified:
            lines += ['### Airflow thực thi', '', *task_table(data['airflow']), '']
    lines += ['### Tải kết quả', '', '| Gói Artifacts | Nội dung |', '|---|---|',
        '| `bang-chung-…` | Log runner/dịch vụ/task, ảnh trình duyệt, receipt và manifest |',
        '| `bao-cao-…` | Báo cáo JSON/Markdown, bảng Gold/metrics và snapshot website (batch) |',
        '| `silver-parquet-…` | Silver đã đọc lại và kiểm chứng (batch) |',
        '| `checkpoint-kafka-…` | Bronze, checkpoint, catalog và thống kê replay (Kafka) |', '',
        'Mở mục **Artifacts** bên dưới để tải ZIP; giữ 14 ngày. HDFS/Kafka/Airflow được dọn sau job. '
        'Artifact lỗi chỉ chứa phần đã sinh để chẩn đoán.', '']
    return '\n'.join(lines), failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['collect', 'index', 'summary'])
    parser.add_argument('--kind', choices=['batch', 'replay'], default='batch')
    parser.add_argument('--job-status', default='success')
    args = parser.parse_args()
    if args.action == 'collect':
        collect()
    elif args.action == 'index':
        index_artifacts(args.kind, args.job_status)
    else:
        text, failed = summary(args.kind, args.job_status)
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a', encoding='utf-8') as stream:
            stream.write(text+'\n')
        if failed:
            raise SystemExit('Missing or invalid integration acceptance report.')


if __name__ == '__main__':
    main()
