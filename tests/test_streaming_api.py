"""Replay API waiting/ready/stale/error states; synthetic local files, no services."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from fastapi.testclient import TestClient
from streaming.replay_event_contract import atomic_publish, envelope
from web.app import create_app
from web.snapshot import PROJECT, write_json
from web_fixtures import make_snapshot


class ReplayAPITests(unittest.TestCase):
    def test_states_and_existing_routes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            snapshot=make_snapshot(root/'web')
            latest=root/'latest.json'
            with TestClient(create_app(snapshot)) as client:
                self.assertEqual(client.get('/api/streaming').json()['status'],'not_configured')
            with patch('socket.create_connection',side_effect=AssertionError('No external network')):
                with TestClient(create_app(snapshot,'fixture-replay',latest)) as client:
                    self.assertEqual(client.get('/api/streaming').json()['status'],'waiting')
                    payload={'run_id':'fixture-replay','source_mode':'historical_replay','logic_version':'replay-v1',
                        'unique_matches':1,'expected_matches':2,'total_records':1,'valid_records':1,
                        'duplicate_records':0,'quarantined_records':0,'updated_at_utc':datetime.now(timezone.utc).isoformat(),
                        'hero_stats':[{'hero_id':i,'picks':1,'wins':int(i<=5),'losses':int(i>5),
                                       'win_rate':float(i<=5),'pick_rate':1.0} for i in range(1,11)],
                        'match_overview':{'total_matches':1,'radiant_wins':1,'dire_wins':0}}
                    atomic_publish(latest,payload)
                    self.assertEqual(client.get('/api/streaming').json()['status'],'ready')
                    payload['updated_at_utc']=(datetime.now(timezone.utc)-timedelta(seconds=60)).isoformat()
                    atomic_publish(latest,payload)
                    self.assertEqual(client.get('/api/streaming').json()['status'],'stale')
                    for broken in ['{"payload":{},"sha256":"wrong"}','null','[]','{']:
                        latest.write_text(broken)
                        error=client.get('/api/streaming').json()
                        self.assertEqual(error['status'],'read_error')
                        self.assertEqual(error['data']['unique_matches'],1)
                    payload['expected_matches']=1
                    atomic_publish(latest,payload)
                    self.assertEqual(client.get('/api/streaming').json()['status'],'complete')
                    self.assertEqual(client.get('/api/dashboard').json()['total_matches'],2)
                    self.assertEqual(client.post('/api/predict',json={'radiant_heroes':[1,2,3,4,5],
                        'dire_heroes':[6,7,8,9,10]}).status_code,200)
            self.assertNotIn('pyspark',sys.modules)


if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ReplayAPITests))
    (PROJECT/'evidence/step6').mkdir(exist_ok=True)
    write_json(PROJECT/'evidence/step6/api_tests.json',{'status':'PASS' if result.wasSuccessful() else 'FAIL',
        'source':'synthetic local snapshot','checks':['not configured','waiting','ready','stale','checksum error with last-good fallback',
        'complete','Gold/prediction unchanged','no Spark import']})
    sys.exit(not result.wasSuccessful())
