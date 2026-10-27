"""Idempotent micro-batch archive and Spark SQL rollups over committed finite replay."""
from functools import reduce
import json
from pathlib import Path
import time
import uuid

from pyspark.sql import functions as F
from analytics.draft_stats import assert_same_table, fs_for
from streaming.common import VERSION, atomic_publish, canonical, digest, envelope, now, unpack, validate_event

ARCHIVE_SCHEMA = '''topic string, partition int, offset long, kafka_timestamp string,
    raw_key string, raw_value string, raw_sha256 string, reason string, match_id long,
    radiant_heroes array<int>, dire_heroes array<int>, radiant_win int'''


class Store:
    def __init__(self, spark, root):
        self.spark, self.root = spark, root.rstrip('/')
        self.fs, self.path = fs_for(spark, self.root)

    def path_for(self, relative):
        return self.spark._jvm.org.apache.hadoop.fs.Path(self.root+'/'+relative)

    def exists(self, relative):
        return self.fs.exists(self.path_for(relative))

    def mkdir(self, relative):
        if not self.fs.mkdirs(self.path_for(relative)):
            raise OSError(f'Cannot create {relative}')

    def write(self, relative, data):
        output = self.fs.create(self.path_for(relative), False)
        try:
            output.write(bytearray(data))
        finally:
            output.close()

    def read(self, relative):
        stream = self.fs.open(self.path_for(relative))
        try:
            output = self.spark._jvm.java.io.ByteArrayOutputStream()
            self.spark._jvm.org.apache.hadoop.io.IOUtils.copyBytes(stream, output, 65536, False)
            return bytes(output.toByteArray())
        finally:
            stream.close()

    def committed(self, directory):
        if not self.exists(directory):
            return []
        return sorted(int(item.getPath().getName()) for item in self.fs.listStatus(self.path_for(directory))
                      if item.isDirectory() and item.getPath().getName().isdigit() and
                      self.exists(directory+'/'+item.getPath().getName()+'/_COMMITTED'))

    def commit(self, staging, destination):
        if self.exists(destination):
            raise FileExistsError(destination)
        parent = destination.rsplit('/', 1)[0]
        self.mkdir(parent)
        if not self.fs.rename(self.path_for(staging), self.path_for(destination)):
            raise OSError('Atomic batch rename failed.')


class ReplaySink:
    def __init__(self, spark, manifest, catalog, serving_path, fault=None):
        self.spark, self.manifest, self.catalog = spark, manifest, catalog
        self.store = Store(spark, manifest['hdfs_root'])
        self.serving_path, self.fault = Path(serving_path), fault
        self.store.mkdir('_staging')
        identity = {key: manifest[key] for key in ['run_id','topic','topic_id','catalog_sha256']}
        identity['logic_version'] = VERSION
        if self.store.exists('consumer_identity.json'):
            if json.loads(self.store.read('consumer_identity.json')) != identity:
                raise ValueError('Consumer identity/logic changed; prepare a new run.')
        else:
            self.store.write('consumer_identity.json', canonical(identity))

    def restore(self):
        # Reconcile an archive commit interrupted before snapshot publication.
        batches = self.store.committed('batches')
        if batches:
            return self.publish(max(batches))
        return None

    def __call__(self, batch, batch_id):
        started = time.monotonic()
        rows = batch.select('topic','partition','offset','timestamp','key','value').limit(501).collect()
        if not rows:
            return
        if len(rows) > 500:
            raise ValueError('Micro-batch exceeds the bounded 500-record validator limit.')
        records = []
        for row in rows:
            raw = bytes(row.value) if not isinstance(row.value,str) else row.value.encode()
            key = (bytes(row.key).decode('utf-8', errors='replace') if not isinstance(row.key,str) else row.key) if row.key is not None else None
            normalized, reason = validate_event(raw, key, self.manifest['run_id'], self.catalog)
            records.append({'topic': row.topic, 'partition': row.partition, 'offset': row.offset,
                'kafka_timestamp': row.timestamp.isoformat() if row.timestamp else None,
                'raw_key': key, 'raw_value': raw.decode('utf-8', errors='replace'),
                'raw_sha256': digest(raw), 'reason': reason,
                'match_id': None, 'radiant_heroes': None, 'dire_heroes': None, 'radiant_win': None,
                **(normalized or {})})
        records.sort(key=lambda row:(row['topic'],row['partition'],row['offset']))
        if len({(r['topic'],r['partition'],r['offset']) for r in records}) != len(records):
            raise ValueError('Duplicate Kafka offsets within a micro-batch.')
        fingerprint = digest(canonical(records))
        destination = f'batches/{batch_id:020d}'
        if self.store.exists(destination):
            previous = json.loads(self.store.read(destination+'/manifest.json'))
            if not self.store.exists(destination+'/_COMMITTED') or previous['fingerprint'] != fingerprint:
                raise ValueError('Batch ID reused with different offsets/payload or incomplete archive.')
        else:
            staging = f'_staging/batch-{batch_id}-{uuid.uuid4().hex}'
            frame = self.spark.createDataFrame(records, ARCHIVE_SCHEMA).cache()
            try:
                valid = frame.where("reason = 'valid'")
                invalid = frame.where("reason != 'valid'")
                for name, subset in [('valid',valid),('quarantine',invalid)]:
                    uri = self.store.root+'/'+staging+'/'+name
                    subset.coalesce(1).write.option('compression','snappy').parquet(uri)
                    assert_same_table(subset,self.spark.read.parquet(uri),['topic','partition','offset'])
                ranges = {}
                for record in records:
                    key = f'{record["topic"]}:{record["partition"]}'
                    ranges.setdefault(key,[]).append(record['offset'])
                meta = {'batch_id':int(batch_id),'records':len(records),
                        'valid_records':sum(r['reason']=='valid' for r in records),
                        'fingerprint':fingerprint,'offset_ranges':{k:[min(v),max(v)] for k,v in ranges.items()},
                        'reason_counts':{reason:sum(r['reason']==reason for r in records) for reason in {r['reason'] for r in records}},
                        'committed_at_utc':now()}
                self.store.write(staging+'/manifest.json',canonical(meta))
                self.store.write(staging+'/_COMMITTED',b'')
                self.store.commit(staging,destination)
            finally:
                frame.unpersist()
        if self.fault:
            self.fault('after_archive_commit')
        # Even a retried committed batch must finish/recover publication.
        payload = self.publish(max(self.store.committed('batches')), started)
        print(json.dumps({'batch_id':int(batch_id),'unique_matches':payload['unique_matches'],
                          'records':payload['total_records'],'duplicates':payload['duplicate_records'],
                          'duration_seconds':round(time.monotonic()-started,2)}),flush=True)

    def publish(self, batch_id, started=None):
        destination = f'statistics/{batch_id:020d}'
        if self.store.exists(destination+'/_COMMITTED'):
            payload = unpack(self.store.read(destination+'/snapshot.json'))
            atomic_publish(self.serving_path,payload)
            return payload
        ids = self.store.committed('batches')
        inventories = [json.loads(self.store.read(f'batches/{i:020d}/manifest.json')) for i in ids]
        frames = [self.spark.read.parquet(self.store.root+f'/batches/{i:020d}/valid') for i in ids]
        valid = reduce(lambda a,b:a.unionByName(b),frames)
        matches = valid.select('match_id','radiant_heroes','dire_heroes','radiant_win').dropDuplicates(['match_id']).cache()
        try:
            matches.createOrReplaceTempView('replay_matches')
            n = matches.count()
            self.spark.sql('''SELECT explode(radiant_heroes) AS hero_id, radiant_win AS team_win FROM replay_matches
                UNION ALL SELECT explode(dire_heroes), 1-radiant_win FROM replay_matches''').createOrReplaceTempView('replay_heroes')
            hero_stats = self.spark.sql(f'''SELECT hero_id, count(*) AS picks, sum(team_win) AS wins,
                count(*)-sum(team_win) AS losses, try_divide(count(*),{n}) AS pick_rate,
                try_divide(sum(team_win),count(*)) AS win_rate,
                CASE WHEN count(*) >= 100 THEN 'eligible' ELSE 'insufficient_sample' END AS sample_status
                FROM replay_heroes GROUP BY hero_id''').cache()
            overview = self.spark.sql('''SELECT count(*) AS total_matches,
                coalesce(sum(cast(radiant_win AS BIGINT)),0) AS radiant_wins,
                coalesce(sum(cast(1-radiant_win AS BIGINT)),0) AS dire_wins,
                try_divide(sum(radiant_win),count(*)) AS radiant_win_rate,
                try_divide(sum(1-radiant_win),count(*)) AS dire_win_rate FROM replay_matches''')
            total = sum(m['records'] for m in inventories)
            accepted = sum(m['valid_records'] for m in inventories)
            heroes = [row.asDict() for row in hero_stats.orderBy('hero_id').collect()]
            if sum(h['picks'] for h in heroes)!=10*n or sum(h['wins'] for h in heroes)!=5*n:
                raise ValueError('Hero appearance accounting failed.')
            payload = {'run_id':self.manifest['run_id'],'source_mode':'historical_replay','logic_version':VERSION,
                'batch_id':int(batch_id),'expected_matches':len(self.catalog),'unique_matches':n,
                'total_records':total,'valid_records':accepted,'duplicate_records':accepted-n,
                'quarantined_records':total-accepted,'coverage':n/len(self.catalog),'min_hero_matches':100,
                'match_overview':overview.first().asDict(),'hero_stats':heroes,'updated_at_utc':now(),
                'status':'complete' if n==len(self.catalog) else 'partial',
                'input_fingerprint_sha256':self.manifest['input_fingerprint_sha256'],
                'latest_offsets':inventories[-1]['offset_ranges'],
                'batch_duration_seconds':round(time.monotonic()-(started or time.monotonic()),2)}
            staging = f'_staging/stats-{batch_id}-{uuid.uuid4().hex}'
            for name,frame in [('hero_stats',hero_stats),('match_overview',overview)]:
                uri = self.store.root+'/'+staging+'/'+name
                frame.coalesce(1).write.option('compression','snappy').parquet(uri)
                keys = ['hero_id'] if name=='hero_stats' else ['total_matches']
                assert_same_table(frame,self.spark.read.parquet(uri),keys)
            self.store.write(staging+'/snapshot.json',canonical(envelope(payload)))
            if unpack(self.store.read(staging+'/snapshot.json')) != payload:
                raise ValueError('Snapshot JSON readback mismatch.')
            self.store.write(staging+'/_COMMITTED',b'')
            self.store.commit(staging,destination)
            atomic_publish(self.serving_path,payload)
            return payload
        finally:
            matches.unpersist()
            if 'hero_stats' in locals():
                hero_stats.unpersist()
