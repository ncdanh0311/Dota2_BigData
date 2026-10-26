#!/usr/bin/env python3
"""Gold SQL integration checks using hand-computable fixtures, no HDFS required."""
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F, types as T
from analytics.draft_stats import (KEYS, assert_same_table, build_tables, check_gold, input_inventory,
                                   report_samples, reserve_run, validate_input)

SCHEMA = T.StructType([
    T.StructField('match_id', T.LongType()),
    T.StructField('radiant_heroes', T.ArrayType(T.IntegerType())),
    T.StructField('dire_heroes', T.ArrayType(T.IntegerType())),
    T.StructField('radiant_win', T.IntegerType()),
])


def rejected(call, exception=ValueError):
    try:
        call()
    except exception:
        return
    raise AssertionError('Expected input / publication rejection.')


def release(tables):
    for frame in tables.values():
        frame.unpersist()


def main():
    spark = (SparkSession.builder.master('local[2]').appName('Dota-Step3-Rule-Tests')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        rows = [(i, [1, 2, 3, 4, 5], [6, 7, 8, 9, 10], i % 2) for i in range(1, 101)]
        rows += [(i, [11, 12, 13, 14, 15], [16, 17, 18, 19, 20], 0) for i in range(101, 200)]
        matches = spark.createDataFrame(rows, SCHEMA)
        assert validate_input(matches) == 199
        tables = build_tables(spark, matches, 'fixture-a')
        quality = check_gold(spark, tables, 199, 100, 100)
        hero1 = tables['hero_stats'].where('hero_id = 1').first()
        assert (hero1.picks, hero1.wins, hero1.losses, hero1.sample_status) == (100, 50, 50, 'eligible')
        assert abs(hero1.pick_rate - 100/199) < 1e-9 and hero1.win_rate == 0.5
        hero11 = tables['hero_stats'].where('hero_id = 11').first()
        assert (hero11.picks, hero11.wins, hero11.sample_status) == (99, 0, 'insufficient_sample')
        hero16 = tables['hero_stats'].where('hero_id = 16').first()
        assert hero16.wins == 99 and hero16.win_rate == 1.0  # Dire inversion
        pair = tables['hero_pair_stats'].where('hero_a=1 AND hero_b=2').first()
        assert (pair.pair_matches, pair.pair_wins, pair.pair_losses) == (100, 50, 50)
        pair99 = tables['hero_pair_stats'].where('hero_a=11 AND hero_b=12').first()
        assert pair99.pair_matches == 99 and pair99.sample_status == 'insufficient_sample'
        assert tables['hero_side_stats'].where('hero_id = 1').count() == 1
        comparisons = report_samples(tables)['top5_side_comparison']
        assert all(row['comparison_status'] == 'not_comparable' and
                   row['delta_radiant_minus_dire'] is None and row['dire'] is None
                   for row in comparisons)  # top five IDs 1..5 are Radiant-only
        ensure_no_false_ranking = report_samples(tables)['top20_hero_win_rate']
        assert all(row['picks'] >= 100 for row in ensure_no_false_ranking)
        # Exact metrics must survive Parquet round-trip, ignoring physical row order.
        with tempfile.TemporaryDirectory(prefix='dota-gold-tests-') as folder:
            for name, frame in tables.items():
                uri = Path(folder, name).as_uri()
                frame.coalesce(1).write.option('compression', 'snappy').parquet(uri)
                assert_same_table(frame, spark.read.parquet(uri), KEYS[name])
                inventory = input_inventory(spark, uri)
                assert len(inventory) == 1 and inventory[0]['bytes'] > 0
            run = Path(folder, 'reserved').as_uri()
            reserve_run(spark, run)
            rejected(lambda: reserve_run(spark, run), FileExistsError)
            assert not Path(folder, 'reserved', '_COMPLETED').exists()
        # Deliberately corrupt a rate: the read-back check must catch it.
        corrupted = tables['hero_stats'].withColumn('win_rate', F.col('win_rate') + 0.1)
        rejected(lambda: assert_same_table(tables['hero_stats'], corrupted, ['hero_id']))
        permuted_rows = [(i, list(reversed(rad)), list(reversed(dire)), label)
                         for i, rad, dire, label in rows]
        permuted = build_tables(spark, spark.createDataFrame(permuted_rows, SCHEMA).repartition(3), 'fixture-b')
        for name in tables:
            # Run identity differs; business metrics must be invariant.
            if name == 'match_overview':
                a = tables[name].drop('analysis_run_id').withColumn('key', F.lit(1))
                b = permuted[name].drop('analysis_run_id').withColumn('key', F.lit(1))
                assert_same_table(a, b, ['key'])
            else:
                assert_same_table(tables[name].drop('analysis_run_id'),
                                  permuted[name].drop('analysis_run_id'), KEYS[name])
        release(permuted)
        # A hero observed on both sides: aggregates and side labels must agree.
        both = spark.createDataFrame([(1, rows[0][1], rows[0][2], 1),
                                      (2, rows[0][2], rows[0][1], 0)], SCHEMA)
        both_tables = build_tables(spark, both, 'fixture-c', 1, 1)
        check_gold(spark, both_tables, 2, 1, 1)
        both_hero = both_tables['hero_stats'].where('hero_id = 1').first()
        assert both_hero.picks == both_hero.wins == 2
        assert all(row['comparison_status'] == 'eligible'
                   for row in report_samples(both_tables)['top5_side_comparison'])
        release(both_tables)
        invalid_sets = [[], [rows[0], rows[0]],
                        [(0, rows[0][1], rows[0][2], 1)],
                        [(1, [1, 1, 3, 4, 5], rows[0][2], 1)],
                        [(1, rows[0][1], [1, 7, 8, 9, 10], 1)],
                        [(1, rows[0][1], [0, 7, 8, 9, 10], 1)],
                        [(1, None, rows[0][2], 1)],
                        [(1, rows[0][1], rows[0][2], 2)],
                        [(1, rows[0][1], [6, 7, 8, 9], 1)]]
        for invalid in invalid_sets:
            rejected(lambda invalid=invalid: validate_input(spark.createDataFrame(invalid, SCHEMA)))
        rejected(lambda: validate_input(matches.withColumn('match_id', F.col('match_id').cast('int'))))
        rejected(lambda: build_tables(spark, matches, 'bad', 0, 100))
        result = {'status': 'PASS', 'fixture_matches': 199, 'quality_checks': quality,
                  'scenarios': ['hand-calculated hero and pair metrics', 'Dire outcome inversion',
                                '99/100 sample threshold', 'insufficient samples retained',
                                'missing side not replaced with zero', 'Parquet round-trip',
                                'nonempty source file inventory',
                                'corrupted output rejected', 'array/partition/order invariance',
                                'both-side hero comparison', 'invalid/empty/duplicate Silver rejected',
                                'schema mismatch rejected', 'run overwrite blocked',
                                'no completion marker for unfinished run']}
        dest = Path(__file__).resolve().parents[1] / 'evidence/step3/rule_tests.json'
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2), flush=True)
        release(tables)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
