#!/usr/bin/env python3
"""Kafka Structured Streaming with HDFS checkpoints and idempotent archive publication."""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession
from analytics.draft_stats import validate_input
from streaming.replay_event_contract import PROJECT, canonical, draft, exclusive, load_catalog, local_run
from streaming.kafka_broker_manager import CONFIG
from streaming.prepare_replay_catalog import topic_identity
from streaming.replay_batch_processor import ReplaySink, Store


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--trigger-seconds',type=int,default=10)
    parser.add_argument('--stop-after-seconds',type=int,default=0,help='Graceful bounded run for recovery verification')
    parser.add_argument('--available-now',action='store_true',help='Drain existing records then stop')
    args=parser.parse_args()
    if args.trigger_seconds < 1 or args.stop_after_seconds < 0:
        parser.error('Invalid trigger/duration.')
    manifest,catalog=load_catalog(args.run_id)
    if topic_identity(manifest['topic']) != manifest['topic_id']:
        raise ValueError('Topic was recreated; use a new replay run.')
    with exclusive(local_run(args.run_id)/'consumer.lock'):
        spark=SparkSession.builder.appName('DotaReplay-'+args.run_id).getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        query=None
        try:
            store=Store(spark,manifest['hdfs_root'])
            if not store.exists('input/_COMPLETED') or json.loads(store.read('input/manifest.json'))!=manifest:
                raise ValueError('HDFS/local replay manifests differ or are incomplete.')
            source=spark.read.parquet(store.root+'/input/matches')
            if validate_input(source)!=len(catalog) or {r.match_id:draft(r.asDict()) for r in source.collect()}!=catalog:
                raise ValueError('HDFS catalog differs from verified local catalog.')
            if store.committed('batches') and not store.exists('checkpoint/offsets'):
                raise ValueError('Archive exists but checkpoint is missing; do not reset this run.')
            sink=ReplaySink(spark,manifest,catalog,PROJECT/'artifacts/streaming'/args.run_id/'latest.json')
            sink.restore()
            incoming=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',CONFIG['bootstrap_servers'])
                .option('subscribe',manifest['topic']).option('startingOffsets','earliest')
                .option('failOnDataLoss','true').option('maxOffsetsPerTrigger','500').load())
            writer=incoming.writeStream.foreachBatch(sink).option('checkpointLocation',store.root+'/checkpoint')
            writer=writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=f'{args.trigger_seconds} seconds')
            query=writer.start()
            print('Streaming started: '+args.run_id,flush=True)
            if args.stop_after_seconds:
                query.awaitTermination(args.stop_after_seconds)
                query.stop()
            else:
                query.awaitTermination()
        except KeyboardInterrupt:
            if query:
                query.stop()
        finally:
            evidence=PROJECT/'evidence/step6'/args.run_id
            evidence.mkdir(parents=True,exist_ok=True)
            if query:
                (evidence/'last_query_progress.json').write_text(json.dumps(query.recentProgress,default=str,indent=2))
                if query.isActive:
                    query.stop()
            spark.stop()


if __name__=='__main__':
    main()
