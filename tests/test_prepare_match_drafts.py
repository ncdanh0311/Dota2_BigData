#!/usr/bin/env python3
"""Small Spark integration test; writes evidence only, no HDFS datasets."""
import csv
import io
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pipeline.prepare_match_drafts import COLUMNS, parse_lines, classify, validate_output


def record(match_id, minute=10, **changes):
    row = dict.fromkeys(COLUMNS, '0')
    row.update(match_id=str(match_id), minute=str(minute), radiant_win='1')
    for i in range(1, 6):
        row[f'rad_h{i}'] = str(i)
        row[f'dire_h{i}'] = str(i + 5)
    row.update({key: str(value) for key, value in changes.items()})
    stream = io.StringIO()
    csv.writer(stream, lineterminator='').writerow([row[column] for column in COLUMNS])
    return stream.getvalue()


def main():
    spark = (SparkSession.builder.master('local[2]').appName('Dota-Step2-Rule-Tests')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        lines = [
            record(1), record(1, 20), record(1),  # repeated valid snapshot + exact duplicate
            record(2), record(2, 20, rad_h1=5, rad_h5=1),  # order alone is irrelevant
            record(3, rad_h1=''), record(3, 20),  # invalid poisons whole match
            record(4, dire_h1=1),  # hero appears on both sides
            record(5, radiant_win=2),
            record(6), record(6, 20, rad_h1=11),  # changing draft
            record(7), record(7, 20, radiant_win=0),  # changing outcome
            record(8), record(8, rad_kills=5),  # same key, different telemetry
            record(9, rad_total_nw=''),  # unused missing metric is allowed
            record(''),  # unknown match
            record(10) + ',999',  # extra CSV field
            record(11, minute=-1), record(12, rad_h1=0),
            record(13, rad_h1=2),  # duplicate within one team
            record(14, minute='oops'),  # malformed numeric required field
        ]
        frame = spark.createDataFrame([(line,) for line in lines], ['_raw_line'])
        parsed = parse_lines(frame).cache()
        assert parsed.count() == len(lines)
        unique, matches, good, bad, quarantine = classify(parsed)
        accepted = {row.match_id for row in good.collect()}
        assert accepted == {1, 2, 9}, accepted
        validate_output(good, 3)
        assert unique.count() == len(lines) - 1
        rejected = {row.match_id: set(row.match_reasons) for row in bad.collect()}
        assert 'HAS_INVALID_SNAPSHOT' in rejected[3]
        assert 'CONFLICTING_DRAFT' in rejected[6]
        assert 'CONFLICTING_LABEL' in rejected[7]
        assert 'CONFLICTING_SNAPSHOT_KEY' in rejected[8]
        assert set(rejected) == {3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14}
        assert quarantine.where('match_id IS NULL').count() == 1
        assert quarantine.where('match_id = 3').count() == 2
        assert matches.count() == good.count() + bad.count()
        result = {'status': 'PASS', 'fixture_rows': len(lines),
                  'accepted_matches': sorted(accepted), 'rejected_matches': sorted(rejected),
                  'scenarios': ['consistent snapshots', 'hero order changes', 'missing hero',
                                'cross-team duplicate hero', 'within-team duplicate hero',
                                'invalid hero ID', 'invalid label', 'changing draft',
                                'changing label', 'exact duplicate', 'conflicting snapshot key',
                                'unused missing metric', 'unidentifiable match', 'extra CSV field',
                                'negative minute', 'invalid numeric minute', 'whole match quarantine']}
        evidence = Path(__file__).resolve().parents[1] / 'evidence' / 'step2'
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / 'rule_tests.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2), flush=True)
        matches.unpersist()
        unique.unpersist()
        parsed.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
