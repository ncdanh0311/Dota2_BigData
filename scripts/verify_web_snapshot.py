#!/usr/bin/env python3
"""Verify a real local serving snapshot without permitting network connections."""
import argparse
from pathlib import Path
import sys
import socket
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from ml.predict_draft import predict
from web.app import create_app
from web.snapshot import PROJECT, load_snapshot, safe_id, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--require-hdfs-down', action='store_true')
    args = parser.parse_args()
    safe_id(args.snapshot_id)
    if args.require_hdfs_down:
        with socket.socket() as probe:
            probe.settimeout(2)
            if probe.connect_ex(('127.0.0.1', 9000)) == 0:
                raise AssertionError('HDFS is still accepting connections on port 9000.')
    path = PROJECT/'artifacts/web'/args.snapshot_id
    checks = 0
    with patch('socket.create_connection', side_effect=AssertionError('External network forbidden')), \
         patch('socket.socket.connect', side_effect=AssertionError('External network forbidden')):
        snapshot = load_snapshot(path)
        with TestClient(create_app(path)) as client:
            assert client.get('/health').json()['snapshot_id'] == args.snapshot_id
            assert client.get('/api/dashboard').json()['match_overview'] == snapshot['dashboard']['match_overview']
            for hero in snapshot['heroes']:
                response = client.get(hero['image_url'])
                assert response.status_code == 200 and len(response.content) > 0
            examples = [([7,38,48,85,106], [11,86,123,128,136]),
                        ([11,86,123,128,136], [7,38,48,85,106])]
            supported = snapshot['bundle']['hero_ids']
            examples += [(supported[i:i+5], supported[i+5:i+10]) for i in range(0,len(supported)-9,5)]
            for radiant, dire in examples:
                body = {'radiant_heroes': radiant, 'dire_heroes': dire}
                expected = predict(snapshot['bundle'], radiant, dire)
                response = client.post('/api/predict', json=body)
                assert response.status_code == 200 and response.json() == expected
                checks += 1
            assert client.post('/api/predict', json={'radiant_heroes': [999999]*5,
                'dire_heroes': [1,2,3,4,5]}).status_code == 422
            for asset in ['/', '/static/style.css', '/static/app.js', '/static/logic.js']:
                assert client.get(asset).status_code == 200
            assert client.get('/model_bundle.json').status_code == 404
            assert 'pyspark' not in sys.modules
    evidence = PROJECT/'evidence/step5'/args.snapshot_id
    write_json(evidence/'runtime_checks.json', {'status': 'PASS', 'source': 'real Gold/model snapshot',
        'snapshot_id': args.snapshot_id, 'api_bundle_parity_cases': checks,
        'hero_image_routes_checked': len(snapshot['heroes']), 'spark_imported': False,
        'hdfs_port_9000_confirmed_down': args.require_hdfs_down,
        'network': 'socket connection creation blocked throughout snapshot loading and TestClient requests',
        'os_network_configuration_changed': False})
    print('Real snapshot API checks passed with network connections blocked.')


if __name__ == '__main__':
    main()
