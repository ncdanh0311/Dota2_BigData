#!/usr/bin/env python3
"""Compare a completed real replay against an independent Python catalog aggregation."""
import argparse
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from streaming.common import PROJECT, load_catalog, unpack, now, safe_id
from web.snapshot import write_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--label',default='final')
    parser.add_argument('--expected-records',type=int)
    args=parser.parse_args()
    safe_id(args.label)
    manifest,catalog=load_catalog(args.run_id)
    payload=unpack((PROJECT/'artifacts/streaming'/args.run_id/'latest.json').read_bytes())
    n=len(catalog)
    assert payload['run_id']==args.run_id and payload['unique_matches']==n
    assert payload['input_fingerprint_sha256']==manifest['input_fingerprint_sha256']
    if args.expected_records is not None:
        assert payload['total_records']==args.expected_records
    picks,wins=Counter(),Counter()
    radiant_wins=0
    for record in catalog.values():
        radiant_wins+=record['radiant_win']
        for side in ['radiant','dire']:
            result=record['radiant_win'] if side=='radiant' else 1-record['radiant_win']
            for hero in record[side+'_heroes']:
                picks[hero]+=1
                wins[hero]+=result
    actual={row['hero_id']:row for row in payload['hero_stats']}
    assert set(actual)==set(picks)
    for hero,row in actual.items():
        assert (row['picks'],row['wins'],row['losses'])==(picks[hero],wins[hero],picks[hero]-wins[hero])
        assert abs(row['pick_rate']-picks[hero]/n)<=1e-9
        assert abs(row['win_rate']-wins[hero]/picks[hero])<=1e-9
        assert row['sample_status']==('eligible' if picks[hero]>=100 else 'insufficient_sample')
    assert payload['match_overview']['radiant_wins']==radiant_wins
    assert payload['match_overview']['dire_wins']==n-radiant_wins
    assert sum(picks.values())==10*n and sum(wins.values())==5*n
    assert payload['duplicate_records']==payload['valid_records']-n
    assert payload['quarantined_records']==0
    evidence=PROJECT/'evidence/step6'/args.run_id
    evidence.mkdir(parents=True,exist_ok=True)
    write_json(evidence/('verification-'+args.label+'.json'),{'status':'PASS','source':'real Kafka replay, independent Python reference',
        'verified_at_utc':now(),'run_id':args.run_id,'unique_matches':n,'total_records':payload['total_records'],
        'duplicate_records':payload['duplicate_records'],'quarantined_records':payload['quarantined_records'],
        'radiant_wins':radiant_wins,'dire_wins':n-radiant_wins,'hero_picks':sum(picks.values()),
        'hero_wins':sum(wins.values()),'hero_rows':len(actual),'batch_id':payload['batch_id'],
        'latest_offsets':payload['latest_offsets'],'catalog_sha256':manifest['catalog_sha256']})
    print(f'PASS: {n} unique matches, {payload["total_records"]} records, {payload["duplicate_records"]} duplicates.')


if __name__=='__main__':
    main()
