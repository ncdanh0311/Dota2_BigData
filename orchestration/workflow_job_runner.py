"""Stdlib workflow adapter. Airflow parsing never imports Spark/Kafka clients."""
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from urllib.parse import urlsplit
import uuid

from web.snapshot import PROJECT, load_snapshot, safe_id
from orchestration.runtime_config import load_config

RAW='hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv'
SILVER='hdfs://localhost:9000/dota/processed/match_drafts_v1/'


def encoded(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,indent=2,allow_nan=False).encode()


def write(path,value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name('.'+path.name+uuid.uuid4().hex)
    temp.write_bytes(encoded(value))
    os.replace(temp,path)


def read(path):
    return json.loads(Path(path).read_bytes())


def identity(dag_id,run_id):
    safe_id(dag_id)
    return 'af-'+hashlib.sha256(f'{dag_id}:{run_id}'.encode()).hexdigest()[:16]


def attempt_id(execution_id,stage,try_number):
    safe_id(stage)
    return safe_id(f'{execution_id}-{stage}-t{int(try_number)}-{uuid.uuid4().hex[:8]}')


def validate_params(kind,params):
    defaults=({'raw_input':RAW,'assets_snapshot_id':'web-demo-v1'} if kind=='batch' else
              {'silver_input':SILVER,'limit':1000,'rate':5,'demo_fail_once':False})
    if set(params)-set(defaults):
        raise ValueError('Unknown DAG parameters: '+str(sorted(set(params)-set(defaults))))
    result={**defaults,**params}
    uri=result['raw_input' if kind=='batch' else 'silver_input']
    split=urlsplit(uri)
    prefix='/dota/raw/' if kind=='batch' else '/dota/processed/'
    if split.scheme!='hdfs' or split.netloc!='localhost:9000' or not split.path.startswith(prefix) or '..' in Path(split.path).parts:
        raise ValueError('Input must be a named project HDFS URI.')
    if kind=='batch':
        if result['assets_snapshot_id']:
            safe_id(result['assets_snapshot_id'])
    elif (type(result['limit']) is not int or not 1<=result['limit']<=90530 or
          type(result['rate']) not in (int,float) or not 0<result['rate']<=1000 or
          type(result['demo_fail_once']) is not bool):
        raise ValueError('Invalid replay limit/rate/demo parameters.')
    return result


def clean_env(runtime):
    env={k:v for k,v in os.environ.items() if not k.startswith('AIRFLOW') and k not in ('VIRTUAL_ENV','PYTHONPATH','PYTHONHOME')}
    env.update(JAVA_HOME=runtime['java_home'],HADOOP_HOME=runtime['hadoop_home'],
        HADOOP_CONF_DIR=runtime['cluster_root']+'/conf/namenode',SPARK_LOCAL_IP='127.0.0.1',
        PYSPARK_PYTHON=runtime['spark_python'],PYSPARK_DRIVER_PYTHON=runtime['spark_python'],
        PATH=os.pathsep.join(dict.fromkeys([runtime['java_home']+'/bin',
            str(Path(runtime['spark_python']).parent), str(Path(runtime['spark_submit']).parent),
            runtime['hadoop_home']+'/bin', '/usr/bin', '/bin', '/usr/local/bin'])))
    return env


class Runner:
    def __init__(self,context,params,kind):
        self.context=context
        self.kind=kind
        self.params=validate_params(kind,params)
        self.execution_id=identity(context['dag_id'],context['run_id'])
        self.directory=PROJECT/'artifacts/airflow'/context['dag_id']/self.execution_id
        self.runtime=load_config('hdfs', PROJECT)
        self.env=clean_env(self.runtime)

    def command(self,argv,attempt):
        logging.info('Launching job: %s',json.dumps(list(map(str,argv))))
        log=self.directory/'attempts'/attempt/'job.log'
        log.parent.mkdir(parents=True,exist_ok=True)
        process=None
        try:
            with log.open('w',encoding='utf-8') as stream:
                process=subprocess.Popen(list(map(str,argv)),cwd=PROJECT,env=self.env,stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,errors='replace',start_new_session=True)
                for line in process.stdout:
                    stream.write(line)
                    stream.flush()
                    logging.info('%s',line.rstrip())
                code=process.wait()
            if code!=0:
                raise RuntimeError(f'Job exited {code}; see {log}')
        finally:
            if process and process.poll() is None:
                os.killpg(process.pid,signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                    process.wait()

    def spark(self,script,args,attempt,kafka=False):
        argv=[self.runtime['spark_submit'],'--master','local[2]','--driver-memory','1g',
            '--conf','spark.driver.host=127.0.0.1','--conf','spark.driver.bindAddress=127.0.0.1',
            '--conf','spark.sql.shuffle.partitions=8','--conf','spark.sql.session.timeZone=UTC',
            '--conf','spark.hadoop.ipc.client.connect.max.retries=1',
            '--conf','spark.hadoop.ipc.client.connect.max.retries.on.timeouts=1']
        if kafka:
            config=load_config('kafka', PROJECT)
            provided='org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api,org.scala-lang:scala-library,org.scala-lang.modules:scala-parallel-collections_2.13,org.slf4j:slf4j-api,org.xerial.snappy:snappy-java,org.lz4:lz4-java,at.yawk.lz4:lz4-java,com.github.luben:zstd-jni'
            argv+=['--conf','spark.jars.ivySettings='+str(PROJECT/'configs/ivy-kafka.xml'),
                   '--exclude-packages',provided,'--packages',config['connector']]
        # Set Spark home from its own interpreter, independent of Airflow's venv.
        self.env['SPARK_HOME']=subprocess.check_output([self.runtime['spark_python'],'-c',
            'import pathlib,pyspark; print(pathlib.Path(pyspark.__file__).parent)'],env=self.env,text=True).strip()
        self.command(argv+[str(PROJECT/script),*map(str,args)],attempt)

    def receipt(self,reference,stage):
        if not isinstance(reference,dict) or set(reference)!={'path','sha256'}:
            raise ValueError('Missing successful receipt: '+stage)
        path=Path(reference['path']).resolve()
        if not path.is_relative_to(self.directory.resolve()):
            raise ValueError('Receipt belongs to another DAG execution.')
        raw=path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=reference['sha256']:
            raise ValueError('Receipt checksum mismatch: '+stage)
        value=json.loads(raw)
        if value['status']!='PASS' or value['stage']!=stage or value['execution_id']!=self.execution_id:
            raise ValueError('Receipt identity/status mismatch: '+stage)
        return value['outputs']

    def run(self,stage,references):
        attempt=attempt_id(self.execution_id,stage,self.context['try_number'])
        started=time.monotonic()
        self.directory.mkdir(parents=True,exist_ok=True)
        try:
            upstream={name:self.receipt(ref,name) for name,ref in references.items()}
            outputs=self.perform(stage,upstream,attempt)
            if stage=='produce_events' and self.params['demo_fail_once'] and self.context['try_number']==1:
                raise RuntimeError('DEMO: producer ACKs complete; fail once before task success to exercise Airflow retry.')
            payload={'status':'PASS','stage':stage,'execution_id':self.execution_id,'attempt_id':attempt,
                'try_number':self.context['try_number'],'dag_id':self.context['dag_id'],'dag_run_id':self.context['run_id'],
                'outputs':outputs,'upstream_receipts':references,'elapsed_seconds':round(time.monotonic()-started,2),
                'finished_at_utc':datetime.now(timezone.utc).isoformat()}
            path=self.directory/'attempts'/attempt/'receipt.json'
            write(path,payload)
            return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        except BaseException as error:
            write(self.directory/'attempts'/attempt/'failure.json',{'stage':stage,'attempt_id':attempt,
                'try_number':self.context['try_number'],'error':str(error),'status':'FAILED'})
            if stage=='verify_and_report':
                self.report('FAILED',{},str(error))
            raise

    def perform(self,stage,upstream,attempt):
        if stage=='preflight':
            with socket.create_connection(('127.0.0.1',9000),timeout=3):
                pass
            if self.kind=='replay':
                with socket.create_connection(('127.0.0.1',9092),timeout=3):
                    pass
            source=self.params['raw_input' if self.kind=='batch' else 'silver_input']
            self.command([self.runtime['hadoop_home']+'/bin/hdfs','dfs','-test','-e',source],attempt)
            if self.kind=='batch' and self.params['assets_snapshot_id']:
                load_snapshot(PROJECT/'artifacts/web'/self.params['assets_snapshot_id'])
            write(self.directory/'run_plan.json',{'dag_id':self.context['dag_id'],'run_id':self.context['run_id'],
                'execution_id':self.execution_id,'kind':self.kind,'params':self.params})
            return {'source':source,'params':self.params}
        if stage=='normalize_silver':
            uri='hdfs://localhost:9000/dota/processed/match_drafts_'+attempt
            self.spark('pipeline/prepare_match_drafts.py',['--input',upstream['preflight']['source'],'--output',uri],attempt)
            report=read(PROJECT/'evidence/step2'/('match_drafts_'+attempt)/'report.json')
            if report['status']!='PASS' or not report['parquet_readback_verified']:
                raise ValueError('Silver did not pass readback.')
            return {'uri':uri,'rows':report['metrics']['valid_matches'],'report_path':str(PROJECT/'evidence/step2'/('match_drafts_'+attempt)/'report.json')}
        if stage=='analyze_gold':
            silver=upstream['normalize_silver']['uri']
            self.spark('analytics/draft_stats.py',['--input',silver,'--run-id',attempt],attempt)
            path=PROJECT/'evidence/step3'/attempt/'report.json'
            report=read(path)
            if report['status']!='PASS' or not report['gold_readback_verified'] or report['input_uri']!=silver:
                raise ValueError('Gold readback/lineage failed.')
            return {'run_id':attempt,'report_path':str(path),'silver_uri':silver,'fingerprint':report['input_fingerprint_sha256']}
        if stage=='train_lr':
            silver=upstream['normalize_silver']['uri']
            self.spark('ml/train_draft_model.py',['--input',silver,'--run-id',attempt,'--seed','42'],attempt)
            path=PROJECT/'evidence/step4'/attempt/'manifest.json'
            manifest=read(path)
            if manifest['status']!='PASS' or manifest['input_uri']!=silver or manifest['input_fingerprint_sha256']!=upstream['analyze_gold']['fingerprint']:
                raise ValueError('Model lineage failed.')
            return {'run_id':attempt,'manifest_path':str(path),'silver_uri':silver,'fingerprint':manifest['input_fingerprint_sha256']}
        if stage=='export_snapshot':
            args=['--gold-run-id',upstream['analyze_gold']['run_id'],
                '--model-run-id',upstream['train_lr']['run_id'],'--snapshot-id',attempt]
            if self.params['assets_snapshot_id']:
                args+=['--hero-assets-from',str(PROJECT/'artifacts/web'/self.params['assets_snapshot_id'])]
            self.spark('web/export_snapshot.py',args,attempt)
            snapshot=load_snapshot(PROJECT/'artifacts/web'/attempt)
            if snapshot['manifest']['input_fingerprint_sha256']!=upstream['train_lr']['fingerprint']:
                raise ValueError('Website snapshot source differs.')
            return {'snapshot_id':attempt,'path':str(PROJECT/'artifacts/web'/attempt)}
        if stage=='prepare_replay':
            self.spark('streaming/prepare_replay_catalog.py',['--input',self.params['silver_input'],'--run-id',attempt,
                '--limit',str(self.params['limit'])],attempt)
            from streaming.replay_event_contract import load_catalog
            manifest,catalog=load_catalog(attempt)
            return {'replay_run_id':attempt,'catalog_sha256':manifest['catalog_sha256'],'rows':len(catalog)}
        if stage=='produce_events':
            replay_id=upstream['prepare_replay']['replay_run_id']
            evidence=PROJECT/'evidence/step6'/replay_id
            before=set(evidence.glob('producer-*.json')) if evidence.exists() else set()
            self.command([os.environ.get('DOTA_STREAMING_PYTHON',str(PROJECT/'.venv-streaming/bin/python')),str(PROJECT/'streaming/publish_match_events.py'),
                '--run-id',replay_id,'--rate',str(self.params['rate'])],attempt)
            added=set(evidence.glob('producer-*.json'))-before
            if len(added)!=1:
                raise ValueError('Producer invocation report missing/ambiguous.')
            path=added.pop()
            report=read(path)
            if report['status']!='PASS' or report['sent']!=upstream['prepare_replay']['rows']:
                raise ValueError('Producer ACK count mismatch.')
            return {'replay_run_id':replay_id,'producer_report':str(path),'acked':report['sent']}
        if stage=='consume_available_now':
            replay_id=upstream['prepare_replay']['replay_run_id']
            self.spark('streaming/consume_match_events.py',['--run-id',replay_id,'--available-now'],attempt,kafka=True)
            return {'replay_run_id':replay_id,'snapshot_path':str(PROJECT/'artifacts/streaming'/replay_id/'latest.json')}
        if stage=='verify_and_report':
            required=(['preflight','normalize_silver','analyze_gold','train_lr','export_snapshot'] if self.kind=='batch' else
                      ['preflight','prepare_replay','produce_events','consume_available_now'])
            if set(upstream)!=set(required):
                raise ValueError('Missing mandatory upstream stages.')
            if self.kind=='batch':
                snapshot=load_snapshot(upstream['export_snapshot']['path'])
                manifest=snapshot['manifest']
                if manifest['gold_run_id']!=upstream['analyze_gold']['run_id'] or manifest['model_run_id']!=upstream['train_lr']['run_id']:
                    raise ValueError('Final snapshot does not match successful attempts.')
                if snapshot['dashboard']['total_matches']!=upstream['normalize_silver']['rows']:
                    raise ValueError('Silver/website counts differ.')
                summary={'snapshot_id':manifest['snapshot_id'],'matches':snapshot['dashboard']['total_matches'],
                    'fingerprint':manifest['input_fingerprint_sha256'],'source_chain':upstream}
            else:
                replay_id=upstream['prepare_replay']['replay_run_id']
                self.command([self.runtime['spark_python'],str(PROJECT/'scripts/verify_replay_statistics.py'),
                    '--run-id',replay_id,'--label',self.execution_id],attempt)
                from streaming.replay_event_contract import unpack
                snapshot=unpack(Path(upstream['consume_available_now']['snapshot_path']).read_bytes())
                summary={k:snapshot[k] for k in ['run_id','unique_matches','total_records','duplicate_records','quarantined_records']}
                summary['source_chain']=upstream
            self.report('PASS',summary)
            return summary
        raise ValueError('Unknown stage: '+stage)

    def report(self,status,summary,error=None):
        payload={'status':status,'dag_id':self.context['dag_id'],'dag_run_id':self.context['run_id'],
            'execution_id':self.execution_id,'params':self.params,'summary':summary,'error':error,
            'generated_at_utc':datetime.now(timezone.utc).isoformat()}
        write(self.directory/'report.json',payload)
        evidence=PROJECT/'evidence/step7'/self.execution_id
        write(evidence/'report.json',payload)
        text=f'# Airflow {self.kind} — {status}\n\nDAG: `{self.context["dag_id"]}`\n\nRun: `{self.context["run_id"]}`\n\n```json\n'+json.dumps(summary,ensure_ascii=False,indent=2)+'\n```\n'
        if error:
            text+='\nError: '+error+'\n'
        (self.directory/'report.md').write_text(text,encoding='utf-8')
        (evidence/'report.md').write_text(text,encoding='utf-8')
        marker=self.directory/'_COMPLETED'
        if status=='PASS':
            marker.touch(exist_ok=True)
        else:
            marker.unlink(missing_ok=True)
