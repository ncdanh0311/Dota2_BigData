"""Owned process-group shutdown: stopped zombies versus still-running children."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.airflow_runtime_manager import process_group_running, finish_owned_ci_group, validate_ci_cleanup


class CleanupTests(unittest.TestCase):
    def test_zombies_are_stopped_but_live_children_are_not(self):
        with tempfile.TemporaryDirectory() as directory:
            proc=Path(directory)
            for pid,state,group in [(10,'Z',10),(11,'S',10),(12,'S',99)]:
                (proc/str(pid)).mkdir()
                (proc/str(pid)/'stat').write_text(f'{pid} (Airflow (worker)) {state} 1 {group} 0 0')
            self.assertTrue(process_group_running(10,proc))
            (proc/'11/stat').write_text('11 (worker) Z 1 10 0 0')
            self.assertFalse(process_group_running(10,proc))
            self.assertTrue(process_group_running(99,proc))

    def test_real_orphaned_child_is_waited_for(self):
        code="import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])"
        parent=subprocess.Popen([sys.executable,'-c',code],start_new_session=True)
        try:
            parent.wait(timeout=10)
            self.assertTrue(process_group_running(parent.pid))
            os.killpg(parent.pid,signal.SIGTERM)
            for _ in range(40):
                if not process_group_running(parent.pid):
                    break
                time.sleep(.05)
            self.assertFalse(process_group_running(parent.pid))
        finally:
            if process_group_running(parent.pid):
                os.killpg(parent.pid,signal.SIGKILL)
            parent.wait(timeout=10)

    def test_invalid_group_is_rejected(self):
        with self.assertRaises(ValueError):
            process_group_running(0)

    def test_force_is_rejected_outside_disposable_ci_root(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}):
            with self.assertRaises(RuntimeError):
                validate_ci_cleanup()
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,
                {'GITHUB_ACTIONS':'true','RUNNER_TEMP':directory}):
            with self.assertRaises(RuntimeError):
                validate_ci_cleanup()

    def test_owned_ci_worker_ignoring_interrupt_is_terminated(self):
        code="import signal,time; signal.signal(signal.SIGINT,signal.SIG_IGN); print('ready',flush=True); time.sleep(30)"
        worker=subprocess.Popen([sys.executable,'-c',code],start_new_session=True,stdout=subprocess.PIPE,text=True)
        try:
            self.assertEqual(worker.stdout.readline().strip(),'ready')
            self.assertTrue(finish_owned_ci_group(worker.pid))
            self.assertFalse(process_group_running(worker.pid))
        finally:
            if process_group_running(worker.pid):
                os.killpg(worker.pid,signal.SIGKILL)
            worker.wait(timeout=10)
            worker.stdout.close()


if __name__=='__main__':
    unittest.main(verbosity=2)
