#!/usr/bin/env python3
"""Freeze a finite Silver catalog and create a dedicated replay topic."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from analytics.draft_stats import assert_same_table, input_inventory, validate_input
from streaming.replay_event_contract import canonical, digest, draft, local_run, now, safe_id
from streaming.kafka_broker_manager import CONFIG, command
from streaming.replay_batch_processor import Store


def topic_identity(topic):
    response = command('kafka-topics.sh','--bootstrap-server',CONFIG['bootstrap_servers'],
                       '--describe','--topic',topic,capture_output=True,text=True).stdout
    identity = re.search(r'TopicId:\s*(\S+)',response)
    if not identity:
        raise ValueError('Kafka topic identity unavailable.')
    return identity.group(1)


def execute(spark,args):
    safe_id(args.run_id)
    if args.limit < 1:
        raise ValueError('Limit must be positive.')
    local = local_run(args.run_id)
    store = Store(spark,args.output_root.rstrip('/')+'/'+args.run_id)
    if local.exists() or store.fs.exists(store.path):
        raise FileExistsError('Replay already exists; choose a new RunId.')
    silver = spark.read.parquet(args.input).cache()
    try:
        source_count = validate_input(silver)
        if args.limit > source_count:
            raise ValueError('Requested replay exceeds the source match count.')
        selected = silver.orderBy('match_id').limit(args.limit).select('match_id',
            F.array_sort('radiant_heroes').alias('radiant_heroes'),
            F.array_sort('dire_heroes').alias('dire_heroes'),'radiant_win').cache()
        rows = [draft(row.asDict()) for row in selected.orderBy('match_id').collect()]
        catalog_bytes = b''.join(canonical(row)+b'\n' for row in rows)
        topic = 'dota.matches.replay.'+args.run_id
        command('kafka-topics.sh','--bootstrap-server',CONFIG['bootstrap_servers'],'--create',
                '--topic',topic,'--partitions','2','--replication-factor','1','--config','retention.ms=604800000')
        manifest = {'run_id':args.run_id,'logic_version':'replay-v1','topic':topic,'topic_id':topic_identity(topic),
            'input_uri':args.input,'input_fingerprint_sha256':hashlib.sha256(json.dumps(input_inventory(spark,args.input),sort_keys=True).encode()).hexdigest(),
            'source_matches':source_count,'expected_matches':len(rows),'selection':'match_id ascending, first Limit rows',
            'catalog_sha256':digest(catalog_bytes),'hdfs_root':store.root,'schema_version':1,'created_at_utc':now(),
            'reference_labels':dict(Counter(str(row['radiant_win']) for row in rows))}
        local.mkdir(parents=True)
        store.mkdir('input')
        selected.coalesce(1).write.option('compression','snappy').parquet(store.root+'/input/matches')
        assert_same_table(selected,spark.read.parquet(store.root+'/input/matches'),['match_id'])
        store.write('input/manifest.json',canonical(manifest))
        store.write('input/_COMPLETED',b'')
        (local/'matches.jsonl').write_bytes(catalog_bytes)
        (local/'manifest.json').write_bytes(canonical(manifest))
        (local/'_COMPLETED').touch()
        print(json.dumps(manifest),flush=True)
    finally:
        silver.unpersist()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--input',default='hdfs://localhost:9000/dota/processed/match_drafts_v1/')
    parser.add_argument('--output-root',default='hdfs://localhost:9000/dota/streaming/replay_v1')
    parser.add_argument('--limit',type=int,default=1000)
    args=parser.parse_args()
    spark=SparkSession.builder.appName('DotaReplayPrepare').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        execute(spark,args)
    finally:
        spark.stop()


if __name__=='__main__':
    main()
