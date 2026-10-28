"""Evidence packaging keeps source files, checksums and origin labels honest."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from package_evidence import package
from write_ci_summary import REPORTS


class EvidenceTests(unittest.TestCase):
    def test_fresh_ci_bundle_and_missing_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            for name in REPORTS['airflow']:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_text(json.dumps({'status':'PASS'}))
            manifest=package(root/'bundle',kind='ci_fixture',suite='airflow',project=root)
            self.assertEqual(manifest['status'],'PASS')
            record=manifest['files'][0]
            self.assertEqual(record['sha256'],hashlib.sha256((root/'bundle'/record['path']).read_bytes()).hexdigest())
            (root/REPORTS['airflow'][0]).unlink()
            incomplete=package(root/'incomplete',kind='ci_fixture',suite='airflow',project=root)
            self.assertEqual(incomplete['status'],'INCOMPLETE_OR_FAILED')
            self.assertEqual(len(incomplete['missing_reports']),1)

    def test_recorded_bundle_is_archive_not_new_ci_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            source=root/'original'
            source.mkdir()
            (source/'report.json').write_text('{"status":"PASS","matches":90530}')
            before=(source/'report.json').read_bytes()
            manifest=package(root/'bundle',kind='recorded_project',source=source,revision='a'*40)
            self.assertEqual(manifest['status'],'ARCHIVED')
            self.assertEqual(manifest['source_kind'],'recorded_project')
            self.assertEqual((source/'report.json').read_bytes(),before)
            self.assertIn('không chạy lại',(root/'bundle/README.md').read_text())
            with self.assertRaises(FileExistsError):
                package(root/'bundle',kind='recorded_project',source=source,revision='a'*40)

    def test_empty_or_runtime_archive_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            source=root/'source'
            source.mkdir()
            with self.assertRaises(ValueError):
                package(root/'empty',kind='recorded_project',source=source,revision='a'*40)
            (source/'simple_auth_manager_passwords.json.generated').write_text('{}')
            with self.assertRaises(ValueError):
                package(root/'private',kind='recorded_project',source=source,revision='a'*40)


if __name__=='__main__':
    unittest.main(verbosity=2)
