"""Absent or corrupt fixture reports must not be presented as successful checks."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

path=Path(__file__).resolve().parents[1]/'scripts/write_ci_summary.py'
spec=importlib.util.spec_from_file_location('write_ci_summary',path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SummaryTests(unittest.TestCase):
    def test_missing_reports_cannot_confirm_success(self):
        with tempfile.TemporaryDirectory() as root:
            text,failed=module.build_summary('airflow','success',root)
            self.assertTrue(failed)
            self.assertIn('NOT GENERATED',text)

    def test_failed_job_preserves_original_failure(self):
        with tempfile.TemporaryDirectory() as root:
            text,failed=module.build_summary('airflow','failure',root)
            self.assertFalse(failed)
            self.assertIn('**failure**',text)

    def test_fresh_pass_and_corrupt_report(self):
        with tempfile.TemporaryDirectory() as root:
            for name in module.REPORTS['airflow']:
                dest=Path(root)/name
                dest.parent.mkdir(parents=True,exist_ok=True)
                dest.write_text(json.dumps({'status':'PASS'}))
            text,failed=module.build_summary('airflow','success',root)
            self.assertFalse(failed)
            self.assertIn('synthetic/local fixtures',text)
            (Path(root)/module.REPORTS['airflow'][0]).write_text('{broken')
            text,failed=module.build_summary('airflow','success',root)
            self.assertTrue(failed)
            self.assertIn('UNREADABLE',text)


if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SummaryTests))
    sys.exit(not result.wasSuccessful())
