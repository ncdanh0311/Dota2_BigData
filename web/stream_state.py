"""Read-only streaming view: no Spark, Kafka or HDFS imports/network calls."""
from datetime import datetime, timezone
from pathlib import Path
from streaming.replay_event_contract import unpack


class StreamingView:
    def __init__(self, run_id=None, path=None):
        self.run_id, self.path, self.last_good = run_id, Path(path) if path else None, None

    def read(self):
        if self.path is None:
            return {'status':'not_configured','run_id':None,'data':None}
        try:
            if not self.path.exists() and self.last_good is None:
                return {'status':'waiting','run_id':self.run_id,'data':None}
            payload=unpack(self.path.read_bytes())
            n,expected=payload['unique_matches'],payload['expected_matches']
            if payload['run_id']!=self.run_id or payload['source_mode']!='historical_replay' or payload['logic_version']!='replay-v1':
                raise ValueError('Wrong replay identity or source mode.')
            counts=[n,expected,payload['total_records'],payload['valid_records'],payload['duplicate_records'],payload['quarantined_records']]
            if any(type(value) is not int or value<0 for value in counts) or not 0<=n<=expected or expected==0:
                raise ValueError('Invalid replay counts.')
            if payload['valid_records']-n!=payload['duplicate_records'] or payload['total_records']-payload['valid_records']!=payload['quarantined_records']:
                raise ValueError('Replay accounting mismatch.')
            rows=payload['hero_stats']
            if len({r['hero_id'] for r in rows})!=len(rows) or sum(r['picks'] for r in rows)!=10*n or sum(r['wins'] for r in rows)!=5*n:
                raise ValueError('Hero aggregate accounting mismatch.')
            for row in rows:
                if row['picks']<=0 or row['wins']+row['losses']!=row['picks'] or min(row['wins'],row['losses'])<0:
                    raise ValueError('Invalid hero counts.')
                if abs(row['win_rate']-row['wins']/row['picks'])>1e-9 or abs(row['pick_rate']-row['picks']/n)>1e-9:
                    raise ValueError('Invalid hero rates.')
            overview=payload['match_overview']
            if overview['total_matches']!=n or overview['radiant_wins']+overview['dire_wins']!=n:
                raise ValueError('Invalid replay overview.')
            updated=datetime.fromisoformat(payload['updated_at_utc'])
            if updated.utcoffset() is None:
                raise ValueError('Snapshot time must have a timezone.')
            self.last_good=payload
            age=max(0,(datetime.now(timezone.utc)-updated).total_seconds())
            status='complete' if n==expected else ('stale' if age>30 else 'ready')
            return {'status':status,'run_id':self.run_id,'age_seconds':round(age,1),'data':payload}
        except (OSError,ValueError,KeyError,TypeError,ZeroDivisionError) as error:
            return {'status':'read_error','run_id':self.run_id,'error':str(error),'data':self.last_good}
