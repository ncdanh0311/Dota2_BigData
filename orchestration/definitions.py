"""Airflow public-SDK definitions; only lightweight imports at DAG parse time."""
from datetime import timedelta
import pendulum
from airflow.sdk import dag, task, Param, get_current_context
from airflow.sdk.exceptions import AirflowFailException


@task(retries=1,retry_delay=timedelta(seconds=30))
def execute_stage(stage,kind,upstream):
    from orchestration.runner import Runner
    context=get_current_context()
    metadata={'dag_id':context['dag'].dag_id,'run_id':context['dag_run'].run_id,
              'try_number':context['ti'].try_number}
    try:
        return Runner(metadata,dict(context['params']),kind).run(stage,upstream)
    except Exception as error:
        if stage=='verify_and_report':
            raise AirflowFailException(str(error)) from error
        raise


def build_batch():
    from orchestration.runner import RAW
    @dag(dag_id='dota_batch_pipeline',schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,
         start_date=pendulum.datetime(2026,10,1,tz='Asia/Ho_Chi_Minh'),dagrun_timeout=timedelta(minutes=60),
         tags=['dota','batch'],params={'raw_input':Param(RAW,type='string'),
             'assets_snapshot_id':Param('web-demo-v1',type='string')})
    def definition():
        p=execute_stage.override(task_id='preflight',execution_timeout=timedelta(minutes=2))('preflight','batch',{})
        s=execute_stage.override(task_id='normalize_silver',pool='dota_spark',execution_timeout=timedelta(minutes=15))('normalize_silver','batch',{'preflight':p})
        g=execute_stage.override(task_id='analyze_gold',pool='dota_spark',execution_timeout=timedelta(minutes=10))('analyze_gold','batch',{'normalize_silver':s})
        m=execute_stage.override(task_id='train_lr',pool='dota_spark',execution_timeout=timedelta(minutes=10))('train_lr','batch',{'normalize_silver':s,'analyze_gold':g})
        w=execute_stage.override(task_id='export_snapshot',pool='dota_spark',execution_timeout=timedelta(minutes=15))('export_snapshot','batch',{'analyze_gold':g,'train_lr':m})
        execute_stage.override(task_id='verify_and_report',trigger_rule='all_done',retries=0,execution_timeout=timedelta(minutes=5))('verify_and_report','batch',{'preflight':p,'normalize_silver':s,'analyze_gold':g,'train_lr':m,'export_snapshot':w})
    return definition()


def build_replay():
    from orchestration.runner import SILVER
    @dag(dag_id='dota_replay_demo',schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,
         start_date=pendulum.datetime(2026,10,1,tz='Asia/Ho_Chi_Minh'),dagrun_timeout=timedelta(minutes=30),
         tags=['dota','kafka','replay'],params={'silver_input':Param(SILVER,type='string'),
             'limit':Param(1000,type='integer',minimum=1,maximum=90530),
             'rate':Param(5,type='number',exclusiveMinimum=0,maximum=1000),
             'demo_fail_once':Param(False,type='boolean')})
    def definition():
        p=execute_stage.override(task_id='preflight',execution_timeout=timedelta(minutes=2))('preflight','replay',{})
        c=execute_stage.override(task_id='prepare_replay',pool='dota_spark',execution_timeout=timedelta(minutes=10))('prepare_replay','replay',{'preflight':p})
        e=execute_stage.override(task_id='produce_events',execution_timeout=timedelta(minutes=10))('produce_events','replay',{'prepare_replay':c})
        s=execute_stage.override(task_id='consume_available_now',pool='dota_spark',execution_timeout=timedelta(minutes=10))('consume_available_now','replay',{'prepare_replay':c,'produce_events':e})
        execute_stage.override(task_id='verify_and_report',trigger_rule='all_done',retries=0,execution_timeout=timedelta(minutes=5))('verify_and_report','replay',{'preflight':p,'prepare_replay':c,'produce_events':e,'consume_available_now':s})
    return definition()
