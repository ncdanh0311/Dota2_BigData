#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F, types as T
from ml.features import (assign_splits, encode_features, fit_vocabulary, reject_unknown_heroes,
                         validate_features, validate_input, validate_splits)

SCHEMA = 'match_id LONG, radiant_heroes ARRAY<INT>, dire_heroes ARRAY<INT>, radiant_win INT'


def fixture(spark, size=200):
    return spark.createDataFrame([(i, [1, 2, 3, 4, 5], [6, 7, 8, 9, 10], i % 2)
                                  for i in range(1, size + 1)], SCHEMA)


def reject(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError('Expected a rejected input.')


def main():
    spark = (SparkSession.builder.master('local[2]').appName('Dota-ML-Feature-Tests')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        matches = fixture(spark)
        assert validate_input(matches) == 200
        assigned = assign_splits(matches).cache()
        counts = validate_splits(assigned, 200)
        actual = {row.match_id: (row.bucket, row.split) for row in assigned.collect()}
        for mid, (bucket, split) in actual.items():
            expected = int(hashlib.sha256(f'42:{mid}'.encode()).hexdigest()[:8], 16) % 10000
            assert bucket == expected
            assert split == ('train' if expected < 7000 else 'validation' if expected < 8500 else 'test')
        shuffled = assign_splits(matches.orderBy(F.desc('match_id')).repartition(3))
        assert actual == {row.match_id: (row.bucket, row.split) for row in shuffled.collect()}
        vocabulary = fit_vocabulary(assigned)
        assert vocabulary == list(range(1, 11))
        encoded = encode_features(assigned, vocabulary).cache()
        validate_features(encoded, 10)
        vector = encoded.first().features.toArray().tolist()
        assert vector == [1.0]*5 + [-1.0]*5
        reversed_order = matches.select('match_id', F.reverse('radiant_heroes').alias('radiant_heroes'),
                                        F.reverse('dire_heroes').alias('dire_heroes'), 'radiant_win')
        assert encode_features(reversed_order.limit(1), vocabulary).first().features.toArray().tolist() == vector
        swapped = matches.select('match_id', F.col('dire_heroes').alias('radiant_heroes'),
                                 F.col('radiant_heroes').alias('dire_heroes'), 'radiant_win')
        assert encode_features(swapped.limit(1), vocabulary).first().features.toArray().tolist() == [-v for v in vector]
        # A new hero observed only in holdout must not enter the fitted mapping.
        holdout_only = assigned.withColumn('radiant_heroes', F.when(F.col('split') == 'test',
                         F.array(*[F.lit(i) for i in [99, 2, 3, 4, 5]])).otherwise(F.col('radiant_heroes')))
        assert fit_vocabulary(holdout_only) == vocabulary
        reject(lambda: reject_unknown_heroes(holdout_only, vocabulary))
        reject(lambda: validate_splits(assign_splits(matches.limit(1)), 1))
        reject(lambda: validate_input(matches.unionByName(matches.limit(1))))
        reject(lambda: validate_input(matches.withColumn('radiant_win', F.lit(2))))
        result = {'status': 'PASS', 'fixture_matches': 200, 'splits': counts,
                  'scenarios': ['SHA-256 split parity', 'row/partition invariance', 'split disjointness',
                                'train-only mapping', 'holdout unknown hero rejected', 'signed vectors',
                                'hero order invariance', 'team swap negates features', 'invalid input rejected']}
        path = Path(__file__).resolve().parents[1] / 'evidence/step4/feature_tests.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2), flush=True)
        encoded.unpersist(); assigned.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
