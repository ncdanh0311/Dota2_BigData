"""API/bundle parity and integrity checks using local synthetic artifacts only."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi.testclient import TestClient
from ml.predict_draft import predict
from web.app import create_app
from web.snapshot import checksum, read_json, write_json
from web_fixtures import bundle, make_snapshot


class APITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = make_snapshot(Path(self.temp.name)/'snapshot')
        self.client = TestClient(create_app(self.root))
        self.client.__enter__()
        self.draft = {'radiant_heroes': [1,2,3,4,5], 'dire_heroes': [6,7,8,9,10]}

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.temp.cleanup()

    def test_parity_and_no_network(self):
        with patch('socket.create_connection', side_effect=AssertionError('Network forbidden')):
            for draft in [self.draft, {'radiant_heroes': [5,4,3,2,1], 'dire_heroes': [10,9,8,7,6]},
                          {'radiant_heroes': [6,7,8,9,10], 'dire_heroes': [1,2,3,4,5]}]:
                expected = predict(bundle(), draft['radiant_heroes'], draft['dire_heroes'])
                response = self.client.post('/api/predict', json=draft)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)
            self.assertEqual(self.client.get('/health').json()['status'], 'ready')
            self.assertEqual(len(self.client.get('/api/heroes').json()), 10)
            self.assertEqual(self.client.get('/api/dashboard').json()['total_matches'], 2)
            self.assertEqual(self.client.get('/hero-images/placeholder.svg').status_code, 200)
        self.assertNotIn('pyspark', sys.modules)

    def test_invalid_drafts(self):
        for ids in [[1,2,3,4], [1,2,3,4,5,11], [True,2,3,4,5], [1.0,2,3,4,5],
                    ['1',2,3,4,5], [-1,2,3,4,5], [1,1,3,4,5], [6,2,3,4,5], [99,2,3,4,5]]:
            self.assertEqual(self.client.post('/api/predict', json={**self.draft, 'radiant_heroes': ids}).status_code, 422)
        self.assertEqual(self.client.post('/api/predict', json={'radiant_heroes': None}).status_code, 422)
        self.assertEqual(self.client.post('/api/predict', content='{').status_code, 422)

    def test_private_artifacts(self):
        for url in ['/model_bundle.json', '/artifacts/web/snapshot/model_bundle.json', '/hero-images/../model_bundle.json']:
            self.assertEqual(self.client.get(url).status_code, 404)

    def test_local_ui_assets(self):
        # The UI is optional before its own commit, then must serve all imports locally.
        from web.app import STATIC
        if not STATIC.is_dir():
            self.skipTest('UI belongs to the next implementation milestone.')
        for url in ['/', '/static/style.css', '/static/app.js', '/static/logic.js']:
            self.assertEqual(self.client.get(url).status_code, 200)
        html = self.client.get('/').text
        import re
        urls = re.findall(r'(?:src|href)="([^"]+)"', html)
        self.assertFalse(any(url.startswith(('http://', 'https://')) for url in urls))

    def test_threshold_and_integrity(self):
        modified = bundle()
        modified['coefficients'] = [0.0]*10
        write_json(self.root/'model_bundle.json', modified)
        with self.assertRaises(ValueError):
            with TestClient(create_app(self.root)):
                pass
        manifest = read_json(self.root/'manifest.json')
        manifest['checksums']['model_bundle.json'] = checksum(self.root/'model_bundle.json')
        write_json(self.root/'manifest.json', manifest)
        with TestClient(create_app(self.root)) as client:
            result = client.post('/api/predict', json=self.draft).json()
            self.assertEqual((result['p_radiant'], result['predicted_side']), (0.5, 'dire'))
        (self.root/'_COMPLETED').unlink()
        with self.assertRaises(ValueError):
            with TestClient(create_app(self.root)):
                pass


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(APITests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    evidence = Path(__file__).resolve().parents[1]/'evidence/step5'
    evidence.mkdir(parents=True, exist_ok=True)
    write_json(evidence/'api_tests.json', {'status': 'PASS' if result.wasSuccessful() else 'FAIL',
        'source': 'synthetic local fixtures', 'tests': result.testsRun, 'spark_imported': 'pyspark' in sys.modules})
    sys.exit(not result.wasSuccessful())
