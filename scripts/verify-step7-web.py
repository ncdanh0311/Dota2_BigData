"""Check the Airflow-produced snapshot through the real web app, without network access."""
import argparse
import json
from pathlib import Path
import socket
import sys
import urllib.request
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from ml.predict_draft import predict
from orchestration.runner import identity, read, write
from web.app import create_app
from web.snapshot import PROJECT, load_snapshot


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-run-id',required=True)
    parser.add_argument('--replay-run-id',required=True)
    parser.add_argument('--live-port',type=int,help='Also verify a running local server while HDFS/Kafka are off')
    args=parser.parse_args()
    batch=read(PROJECT/'evidence/step7'/identity('dota_batch_pipeline',args.batch_run_id)/'report.json')
    replay=read(PROJECT/'evidence/step7'/identity('dota_replay_demo',args.replay_run_id)/'report.json')
    path=Path(batch['summary']['source_chain']['export_snapshot']['path'])
    snapshot=load_snapshot(path)
    replay_id=replay['summary']['run_id']
    with patch('socket.create_connection',side_effect=AssertionError('Network access forbidden')):
        with TestClient(create_app(path,replay_run_id=replay_id,
                        streaming_path=PROJECT/'artifacts/streaming'/replay_id/'latest.json')) as client:
            health=client.get('/health').json()
            assert health['model_run_id']==snapshot['manifest']['model_run_id']
            assert client.get('/api/dashboard').json()['total_matches']==90530
            heroes=client.get('/api/heroes').json()
            assert len(heroes)==127
            for url in {hero['image_url'] for hero in heroes}:
                assert client.get(url).status_code==200
            radiant=[7,38,48,85,106]
            dire=[11,86,123,128,136]
            results=[]
            for rad,di in [(radiant,dire),(radiant[::-1],dire[::-1]),(dire,radiant)]:
                response=client.post('/api/predict',json={'radiant_heroes':rad,'dire_heroes':di})
                assert response.status_code==200
                assert response.json()==predict(snapshot['bundle'],rad,di)
                results.append(response.json())
            view=client.get('/api/streaming').json()
            assert view['status']=='complete'
            streaming=view['data']
            assert streaming['unique_matches']==replay['summary']['unique_matches']
            assert streaming['total_records']==replay['summary']['total_records']
    assert 'pyspark' not in sys.modules and 'airflow' not in sys.modules
    live_checks=None
    if args.live_port:
        if not 1<=args.live_port<=65535:
            parser.error('Invalid local port')
        for port in (9000,9092):
            with socket.socket() as probe:
                probe.settimeout(1)
                assert probe.connect_ex(('127.0.0.1',port))!=0,f'Dependency port {port} is still open'
        base=f'http://127.0.0.1:{args.live_port}'
        def request(endpoint,payload=None):
            req=urllib.request.Request(base+endpoint,
                data=json.dumps(payload).encode() if payload is not None else None,
                headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=15) as response:
                return json.load(response)
        assert request('/health')==health
        assert request('/api/dashboard')['total_matches']==90530
        assert request('/api/streaming')['data']['total_records']==streaming['total_records']
        for result in results:
            actual=request('/api/predict',{key:result[key] for key in ('radiant_heroes','dire_heroes')})
            assert actual==result
        live_checks={'url':base,'status':'PASS','hdfs_kafka_ports_closed':[9000,9092],
                     'checks':['health','dashboard','streaming','three predictions vs bundle']}
    write(PROJECT/'evidence/step7/web_snapshot_checks.json',{'status':'PASS',
        'source':'real Airflow output, in-process ASGI requests; sockets denied',
        'health':health,'heroes':len(heroes),'local_image_urls':len({hero['image_url'] for hero in heroes}),
        'prediction_parity':results,'replay_run_id':replay['summary']['run_id'],
        'streaming_counts':{key:streaming[key] for key in ('unique_matches','total_records','duplicate_records')},
        'spark_imported':False,'airflow_imported':False,'live_http_checks':live_checks})
    print('PASS: new snapshot, model parity and replay served without network/Spark/Airflow imports.')


if __name__=='__main__':
    main()
