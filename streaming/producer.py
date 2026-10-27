#!/usr/bin/env python3
"""Replay an immutable catalog; re-running deliberately reuses event IDs."""
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer
from streaming.common import PROJECT, canonical, event_for, exclusive, load_catalog, local_run, now
from streaming.kafka_lab import CONFIG


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--rate',type=float,default=5)
    parser.add_argument('--repeat',type=int,default=1,help='Explicit duplicate/recovery test; default one pass')
    args=parser.parse_args()
    if not 0 < args.rate <= 1000 or args.repeat < 1:
        parser.error('Rate must be in (0,1000]; repeat must be positive.')
    manifest,catalog=load_catalog(args.run_id)
    report={'run_id':args.run_id,'started_at_utc':now(),'rate':args.rate,'sent':0,'offsets':{},'status':'RUNNING'}
    started=time.monotonic()
    evidence=PROJECT/'evidence/step6'/args.run_id
    evidence.mkdir(parents=True,exist_ok=True)
    report_path=evidence/('producer-'+uuid.uuid4().hex[:8]+'.json')
    producer=None
    try:
        with exclusive(local_run(args.run_id)/'producer.lock'):
            producer=KafkaProducer(bootstrap_servers=CONFIG['bootstrap_servers'],enable_idempotence=True,
                acks='all',max_in_flight_requests_per_connection=1,delivery_timeout_ms=30000,
                request_timeout_ms=10000,max_block_ms=15000)
            for _ in range(args.repeat):
                for record in catalog.values():
                    event=event_for(args.run_id,record)
                    acknowledged=producer.send(manifest['topic'],key=event['event_id'].encode(),value=canonical(event)).get(timeout=35)
                    report['sent']+=1
                    key=str(acknowledged.partition)
                    report['offsets'].setdefault(key,{'first':acknowledged.offset,'last':acknowledged.offset,'records':0})
                    report['offsets'][key]['last']=acknowledged.offset
                    report['offsets'][key]['records']+=1
                    if report['sent']%100==0:
                        print(f'Acknowledged {report["sent"]} events',flush=True)
                    time.sleep(1/args.rate)
            producer.flush(timeout=35)
            report['status']='PASS'
    except BaseException as error:
        report['status']='INTERRUPTED' if isinstance(error,KeyboardInterrupt) else 'FAIL'
        report['error']=str(error)
        raise
    finally:
        if producer:
            producer.close(timeout=5)
        report.update(finished_at_utc=now(),elapsed_seconds=round(time.monotonic()-started,2))
        report_path.write_bytes(canonical(report))
        print(json.dumps(report),flush=True)


if __name__=='__main__':
    main()
