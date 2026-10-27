#!/usr/bin/env python3
"""Real Structured Streaming file-source recovery using the production sink, no Kafka/HDFS."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession
from streaming.common import canonical, digest, event_for, unpack, validate_event
from streaming.sink import ReplaySink
from web.snapshot import PROJECT, write_json


def main():
    spark=SparkSession.builder.master('local[2]').appName('ReplayRecoveryFixture').config('spark.sql.shuffle.partitions','2').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    run_id='fixture-replay'
    catalog={1:{'match_id':1,'radiant_heroes':[1,2,3,4,5],'dire_heroes':[6,7,8,9,10],'radiant_win':1},
             2:{'match_id':2,'radiant_heroes':[6,7,8,9,10],'dire_heroes':[1,2,3,4,5],'radiant_win':0}}
    good=event_for(run_id,catalog[1])
    assert validate_event(canonical(good),good['event_id'],run_id,catalog)[1]=='valid'
    reordered={**good,'radiant_heroes':list(reversed(good['radiant_heroes']))}
    assert validate_event(canonical(reordered),good['event_id'],run_id,catalog)[1]=='valid'
    bads=[b'{',canonical({**good,'radiant_win':True}),canonical({**good,'radiant_win':2}),
          canonical({**good,'radiant_heroes':[1,1,3,4,5]}),canonical({**good,'match_id':99,'event_id':run_id+':99'}),
          canonical({**good,'radiant_heroes':[1,2,3,4]}),canonical({**good,'radiant_win':0}),
          canonical({**good,'emitted_at':'2026-01-01'}),canonical({**good,'schema_version':True}),
          canonical({**good,'replay_run_id':'other'}),canonical({**good,'radiant_heroes':[True,2,3,4,5]})]
    for raw in bads:
        assert validate_event(raw,good['event_id'],run_id,catalog)[1]!='valid'
    assert validate_event(canonical(good),'wrong-key',run_id,catalog)[1]=='invalid_key'
    unknown={**good,'match_id':99,'event_id':run_id+':99'}
    assert validate_event(canonical(unknown),unknown['event_id'],run_id,catalog)[1]=='not_in_catalog'
    try:
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            inbox=root/'inbox'
            inbox.mkdir()
            serving=root/'serving/latest.json'
            manifest={'run_id':run_id,'topic':'fixture-topic','topic_id':'fixture-topic-id',
                'catalog_sha256':digest(canonical(catalog)),'input_fingerprint_sha256':'fixture-fingerprint',
                'hdfs_root':str(root/'archive')}
            def record(offset,event):
                return {'topic':'fixture-topic','partition':offset%2,'offset':offset,
                    'timestamp':'2026-10-27T00:00:00Z','key':event.get('event_id',good['event_id']) if isinstance(event,dict) else good['event_id'],
                    'value':canonical(event).decode() if isinstance(event,dict) else event}
            def place(name,records):
                (inbox/name).write_text(''.join(json.dumps(row)+'\n' for row in records))
            schema='topic string, partition int, offset long, timestamp timestamp, key string, value string'
            incoming=spark.readStream.schema(schema).option('maxFilesPerTrigger',1).json(str(inbox))
            checkpoint=str(root/'checkpoint')
            def run(sink,expected_failure=False):
                query=incoming.writeStream.foreachBatch(sink).option('checkpointLocation',checkpoint).trigger(availableNow=True).start()
                try:
                    query.awaitTermination(90)
                    assert not expected_failure, 'Expected failure injection'
                except Exception:
                    if not expected_failure:
                        raise
                finally:
                    if query.isActive:
                        query.stop()
            first=[record(0,good),record(1,good),record(2,'{'),record(3,{**good,'radiant_win':0})]
            place('first.json',first)
            def fault(stage):
                raise RuntimeError('EXPECTED fixture failure after archive commit')
            broken=ReplaySink(spark,manifest,catalog,serving,fault=fault)
            run(broken,expected_failure=True)
            assert broken.store.committed('batches')==[0] and not serving.exists()
            recovered=ReplaySink(spark,manifest,catalog,serving)
            run(recovered)
            before=unpack(serving.read_bytes())
            assert (before['total_records'],before['unique_matches'],before['duplicate_records'],before['quarantined_records'])==(4,1,1,2)
            assert before['hero_stats'][0]['wins']==1 and before['match_overview']['radiant_wins']==1
            place('second.json',[record(4,event_for(run_id,catalog[2])),record(5,good)])
            run(recovered)
            after=unpack(serving.read_bytes())
            assert (after['total_records'],after['unique_matches'],after['duplicate_records'],after['quarantined_records'])==(6,2,2,2)
            assert after['match_overview']['radiant_wins']==1 and after['match_overview']['dire_wins']==1
            assert sum(r['picks'] for r in after['hero_stats'])==20
            assert sum(r['wins'] for r in after['hero_stats'])==10
            # Same Kafka input + batchId may be retried without changing committed counts.
            recovered(spark.read.schema(schema).json(str(inbox/'first.json')),0)
            assert unpack(serving.read_bytes())==after
            serving.unlink()
            assert recovered.restore()==after and unpack(serving.read_bytes())==after
            corrupted=json.loads(serving.read_bytes())
            corrupted['payload']['unique_matches']=99
            try:
                unpack(json.dumps(corrupted))
            except ValueError:
                pass
            else:
                raise AssertionError('Corrupt checksum accepted')
        (PROJECT/'evidence/step6').mkdir(exist_ok=True)
        write_json(PROJECT/'evidence/step6/rule_tests.json',{'status':'PASS','source':'synthetic file-stream with real checkpoint',
            'checks':['strict events/catalog/UTC','order-insensitive heroes','quarantine','hand-calculated totals',
                      'failure after archive commit','checkpoint restart','same batch retry','atomic snapshot recovery/checksum']})
    finally:
        spark.stop()


if __name__=='__main__':
    main()
