"""Workflow adapter contracts, receipts, cancellation and offline assets; no Airflow/Spark services."""
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from orchestration.runner import Runner, identity, attempt_id, validate_params, write
from web.hero_assets import reuse_heroes
from web.snapshot import PROJECT, write_json
from web_fixtures import make_snapshot


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        write(self.root/'configs/hdfs-wsl.json',{'java_home':'/java','hadoop_home':'/hadoop',
              'cluster_root':'/cluster','spark_python':sys.executable,'spark_submit':'/spark-submit'})
        self.project=patch('orchestration.runner.PROJECT',self.root)
        self.project.start()
        self.context={'dag_id':'fixture_dag','run_id':'manual__fixture','try_number':1}

    def tearDown(self):
        self.project.stop()
        self.temp.cleanup()

    def test_names_and_validation(self):
        value=identity('fixture_dag','manual__fixture')
        self.assertEqual(value,identity('fixture_dag','manual__fixture'))
        self.assertNotEqual(attempt_id(value,'gold',1),attempt_id(value,'gold',1))
        for params in [{'limit':True},{'limit':0},{'rate':float('nan')},{'demo_fail_once':'yes'},
                       {'silver_input':'hdfs://localhost:9000/dota/processed/../raw/test'}, {'oops':1}]:
            with self.assertRaises(ValueError):
                validate_params('replay',params)

    def test_receipt_and_failed_finalizer(self):
        runner=Runner(self.context,{},'batch')
        with patch.object(runner,'perform',return_value={'value':42}):
            reference=runner.run('preflight',{})
        self.assertEqual(runner.receipt(reference,'preflight'),{'value':42})
        Path(reference['path']).write_text('{}')
        with self.assertRaises(ValueError):
            runner.receipt(reference,'preflight')
        with self.assertRaises(ValueError):
            runner.run('verify_and_report',{'preflight':None})
        self.assertEqual(json.loads((runner.directory/'report.json').read_text())['status'],'FAILED')
        self.assertFalse((runner.directory/'_COMPLETED').exists())

    def test_retry_preserves_catalog_and_creates_new_attempt(self):
        first=Runner(self.context,{'demo_fail_once':True},'replay')
        with patch.object(first,'perform',return_value={'replay_run_id':'same-catalog'}):
            prepared=first.run('prepare_replay',{})
        seen=[]
        def produce(stage,upstream,attempt):
            seen.append((upstream['prepare_replay']['replay_run_id'],attempt))
            return {'acked':1000}
        with patch.object(first,'perform',side_effect=produce):
            with self.assertRaisesRegex(RuntimeError,'DEMO'):
                first.run('produce_events',{'prepare_replay':prepared})
        second=Runner({**self.context,'try_number':2},{'demo_fail_once':True},'replay')
        with patch.object(second,'perform',side_effect=produce):
            result=second.run('produce_events',{'prepare_replay':prepared})
        self.assertEqual([row[0] for row in seen],['same-catalog','same-catalog'])
        self.assertNotEqual(seen[0][1],seen[1][1])
        self.assertTrue((first.directory/'attempts'/seen[0][1]/'failure.json').exists())
        self.assertEqual(second.receipt(result,'produce_events'),{'acked':1000})

    def test_subprocess_cancellation(self):
        runner=Runner(self.context,{},'batch')
        def timeout(*args):
            raise TimeoutError('fixture task timeout')
        previous=signal.signal(signal.SIGALRM,timeout)
        try:
            signal.alarm(2)
            with self.assertRaises(TimeoutError):
                runner.command([sys.executable,'-c','import os,time; print(os.getpid(),flush=True); time.sleep(30)'],'cancel-fixture')
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM,previous)
        pid=int((runner.directory/'attempts/cancel-fixture/job.log').read_text().strip())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid,0)

    def test_assets_are_offline_and_checked(self):
        source=make_snapshot(self.root/'source')
        target=self.root/'copy'
        target.mkdir()
        with patch('socket.create_connection',side_effect=AssertionError('Network forbidden')):
            heroes,metadata=reuse_heroes(target,range(1,11),{1,2,3},source)
        self.assertEqual([row['id'] for row in heroes if row['supported']],[1,2,3])
        self.assertEqual(metadata['asset_source_snapshot'],'fixture-web')
        self.assertEqual((source/'images/placeholder.svg').read_bytes(),(target/'images/placeholder.svg').read_bytes())
        with self.assertRaisesRegex(ValueError,'missing IDs'):
            reuse_heroes(self.root/'missing',{99},set(),source)
        (source/'heroes.json').write_text('[]')
        with self.assertRaises(ValueError):
            reuse_heroes(self.root/'broken',{1},set(),source)
        self.assertNotIn('pyspark',sys.modules)


if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(RunnerTests))
    (PROJECT/'evidence/step7').mkdir(parents=True,exist_ok=True)
    write_json(PROJECT/'evidence/step7/runner_tests.json',{'status':'PASS' if result.wasSuccessful() else 'FAIL',
        'source':'synthetic local fixture','tests':result.testsRun})
    sys.exit(not result.wasSuccessful())
