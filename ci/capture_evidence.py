"""Capture real HDFS/website pages and render Airflow API task states with Playwright."""
import argparse
import html
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request

from playwright.sync_api import sync_playwright
from web.snapshot import PROJECT, load_snapshot
from ml.predict_draft import predict

OUT = PROJECT/'artifacts/integration'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=['batch', 'replay'], required=True)
    args = parser.parse_args()
    target = OUT/'screenshots'
    target.mkdir(parents=True, exist_ok=True)
    reports = OUT/'reports' if args.kind == 'batch' else OUT/'baseline/reports'
    snapshot = load_snapshot(reports/'website')
    snapshot_id = snapshot['manifest']['snapshot_id']
    local = PROJECT/'artifacts/web'/snapshot_id
    if not local.exists():
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(reports/'website', local)
    argv = [sys.executable, '-m', 'web.serve', '--snapshot-id', snapshot_id]
    if args.kind == 'replay':
        replay = json.loads((OUT/'reports/replay.json').read_bytes())
        argv += ['--replay-run-id', replay['summary']['run_id']]
    with (OUT/'logs/website.log').open('w') as log:
        process = subprocess.Popen(argv, stdout=log, stderr=log)
        try:
            for _ in range(60):
                try:
                    with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2) as response:
                        assert response.status == 200
                    break
                except OSError:
                    time.sleep(1)
            else:
                raise TimeoutError('Website readiness timeout.')
            with sync_playwright() as p:
                browser = p.chromium.launch(channel='chrome', headless=True, args=['--no-sandbox'])
                page = browser.new_page(viewport={'width': 1440, 'height': 1000}, device_scale_factor=1)
                page.goto('http://127.0.0.1:9870/dfshealth.html#tab-datanode')
                # "Live Nodes" belongs to the hidden Overview tab; wait for this tab's table.
                page.locator('#table-datanodes').wait_for(state='visible')
                info = page.request.get('http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo').json()
                assert len(json.loads(info['beans'][0]['LiveNodes'])) == 3
                assert page.locator('#table-datanodes tbody tr').count() == 3
                page.screenshot(path=str(target/'hdfs-three-datanodes.png'), full_page=True)
                page.goto('http://127.0.0.1:8000')
                page.locator('#stats-page').wait_for(state='visible')
                page.screenshot(path=str(target/'website-statistics.png'), full_page=True)
                page.locator('[data-tab="draft"]').click()
                page.locator('#demo-button').click()
                page.locator('#predict-button').click()
                page.locator('#prediction-result').wait_for(state='visible')
                result = page.request.post('http://127.0.0.1:8000/api/predict', data={
                    'radiant_heroes': [7,38,48,85,106], 'dire_heroes': [11,86,123,128,136]}).json()
                assert result == predict(snapshot['bundle'], [7,38,48,85,106], [11,86,123,128,136])
                page.screenshot(path=str(target/'website-prediction.png'), full_page=True)
                if args.kind == 'replay':
                    page.locator('[data-tab="replay"]').click()
                    page.locator('#replay-content').wait_for(state='visible')
                    page.screenshot(path=str(target/'website-kafka-replay.png'), full_page=True)
                data = json.loads((OUT/'reports'/f'{args.kind}.json').read_bytes())
                # Explicitly a rendered API report, never represented as Airflow Graph/Grid UI.
                rows = ''.join('<tr>'+''.join(f'<td>{html.escape(str(row.get(k)))}</td>'
                    for k in ['task_id', 'state', 'try_number', 'duration'])+'</tr>' for row in data['airflow']['tasks'])
                page.set_content('<html lang="vi"><meta charset="utf-8"><style>body{background:#101720;color:#e0e9f2;font:20px sans-serif;padding:32px}td,th{padding:18px;border-bottom:1px solid #435161}table{border-collapse:collapse}</style>'
                    '<h1>Airflow — trạng thái task từ REST API</h1><p>'+html.escape(data['airflow']['run_id'])+
                    '</p><table><tr><th>Task</th><th>State</th><th>Try</th><th>Seconds</th></tr>'+rows+'</table></html>')
                page.screenshot(path=str(target/'airflow-task-report.png'), full_page=True)
                browser.close()
            (OUT/'reports/browser.json').write_text(json.dumps({'status': 'PASS', 'source_kind': 'github_integration',
                'checks': ['real HDFS page', 'website statistics', 'draft demo click/predict', 'API/bundle parity'],
                'airflow_image': 'Rendered REST task report, not a Graph/Grid screenshot'}, indent=2))
        finally:
            process.terminate()
            process.wait(timeout=30)


if __name__ == '__main__':
    main()
