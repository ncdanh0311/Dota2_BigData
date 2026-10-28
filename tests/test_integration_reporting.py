"""CI config isolation, provenance, failure reporting and artifact transfer integrity."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci.prepare_runtime import configure, bootstrap, fetch
from ci.publish_results import summary, index_artifacts, verify_artifact, redact, collect
from orchestration.runtime_config import load_config


class IntegrationReportingTests(unittest.TestCase):
    def test_installer_refuses_local_machine(self):
        with patch.dict(os.environ, {'GITHUB_ACTIONS': 'false'}):
            with self.assertRaisesRegex(RuntimeError, 'disposable'):
                bootstrap()

    def test_official_mirror_fallback_is_bounded(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory, patch('ci.prepare_runtime.subprocess.run',
                side_effect=[SimpleNamespace(returncode=28), SimpleNamespace(returncode=0)]) as command:
            target=Path(directory)/'download'
            self.assertEqual(fetch('https://dlcdn.apache.org/file', target,
                                   'https://archive.apache.org/file'), target)
            self.assertEqual(command.call_count, 2)
            self.assertEqual(command.call_args.args[0][-1], 'https://archive.apache.org/file')

    def test_explicit_runtime_isolated_from_wsl_default(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            configs = configure(root/'ci', '/java', '/python/bin/python', '/python/bin/spark-submit')
            (root/'configs').mkdir()
            default = root/'configs/hdfs-wsl.json'
            default.write_text('{"cluster_root":"/original"}')
            override = root/'hdfs.json'
            override.write_text(json.dumps(configs['hdfs']))
            with patch.dict(os.environ, {'DOTA_HDFS_CONFIG': str(override)}):
                self.assertEqual(load_config('hdfs', root), configs['hdfs'])
            self.assertEqual(json.loads(default.read_text()), {'cluster_root': '/original'})

    def test_missing_acceptance_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as directory:
            text, failed = summary('batch', 'success', directory)
            self.assertTrue(failed)
            self.assertIn('Chưa có báo cáo nghiệm thu', text)
            text, failed = summary('replay', 'failure', directory)
            self.assertFalse(failed)
            self.assertIn('**failure**', text)

    def test_replay_table_from_current_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'reports').mkdir()
            report = {'status': 'PASS', 'source_kind': 'github_integration',
                'summary': {'total_records': 20, 'unique_matches': 10, 'duplicate_records': 10, 'quarantined_records': 0},
                'hero_picks': 100, 'hero_wins': 50,
                'airflow': {'tasks': [{'task_id': 'produce_events', 'state': 'success', 'try_number': 2, 'duration': 1.5}]}}
            (root/'reports/replay.json').write_text(json.dumps(report))
            text, failed = summary('replay', 'success', root)
            self.assertFalse(failed)
            self.assertIn('| 20 | 10 | 10 | 0 | 100 | 50 |', text)
            self.assertIn('produce_events', text)
            report['source_kind'] = 'recorded_project'
            (root/'reports/replay.json').write_text(json.dumps(report))
            self.assertTrue(summary('replay', 'success', root)[1])

    def test_transfer_checksums_and_run_identity(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'GITHUB_RUN_ID': '123'}):
            root = Path(directory)
            (root/'silver').mkdir()
            data = root/'silver/data.parquet'
            data.write_bytes(b'parquet-test-placeholder')
            with patch('ci.publish_results.OUT', root):
                index_artifacts('batch', 'success')
            self.assertEqual(verify_artifact(root/'silver')['status'], 'success')
            with patch.dict(os.environ, {'GITHUB_RUN_ID': '999'}):
                with self.assertRaises(ValueError):
                    verify_artifact(root/'silver')
            data.write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                verify_artifact(root/'silver')

    def test_private_runtime_values_redacted(self):
        self.assertEqual(redact('password=private-value', ['private-value']), 'password=[REDACTED]')

    def test_bootstrap_failure_still_collects_runner_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'logs').mkdir()
            (root/'logs/dependencies.log').write_text('dependency failed')
            with patch('ci.publish_results.OUT', root), patch.dict(os.environ, {}, clear=True):
                collect()
            self.assertEqual((root/'evidence/runner-logs/dependencies.log').read_text(), 'dependency failed')


if __name__ == '__main__':
    unittest.main(verbosity=2)
