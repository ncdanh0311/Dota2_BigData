"""Parse both DAGs with the pinned Airflow SDK, without launching any external job."""
import importlib.util
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from web.snapshot import PROJECT, write_json


def main():
    # Airflow/croniter performs a one-time platform probe (`file`) during import.
    # Warm up the SDK itself before forbidding external work by our DAG modules.
    import airflow.sdk.definitions.timetables.interval
    specs={'dota_batch_pipeline':{'preflight','normalize_silver','analyze_gold','train_lr','export_snapshot','verify_and_report'},
           'dota_replay_demo':{'preflight','prepare_replay','produce_events','consume_available_now','verify_and_report'}}
    for name,expected in specs.items():
        path=PROJECT/'orchestration/dags'/(name+'.py')
        spec=importlib.util.spec_from_file_location(name,path)
        module=importlib.util.module_from_spec(spec)
        with patch('subprocess.Popen',side_effect=AssertionError('Jobs forbidden at DAG parse')), \
             patch('socket.create_connection',side_effect=AssertionError('Network forbidden at DAG parse')):
            spec.loader.exec_module(module)
        dag=module.dag
        assert dag.dag_id==name and dag.schedule is None and not dag.catchup
        assert dag.max_active_runs==1 and dag.max_active_tasks==1
        assert set(dag.task_dict)==expected
        terminal=dag.task_dict['verify_and_report']
        assert terminal.trigger_rule=='all_done' and terminal.retries==0
        assert set(terminal.upstream_task_ids)==expected-{'verify_and_report'}
        for task in dag.tasks:
            if task.task_id in {'normalize_silver','analyze_gold','train_lr','export_snapshot','prepare_replay','consume_available_now'}:
                assert task.pool=='dota_spark'
    assert 'pyspark' not in sys.modules
    (PROJECT/'evidence/step7').mkdir(parents=True,exist_ok=True)
    write_json(PROJECT/'evidence/step7/dag_tests.json',{'status':'PASS','source':'Airflow SDK parse, no services',
        'dags':list(specs),'checks':['task graph','manual schedule','concurrency/pool','failure-sensitive finalizer','no job/network at parse']})
    print('Both Airflow DAGs parsed and validated.')


if __name__=='__main__':
    main()
