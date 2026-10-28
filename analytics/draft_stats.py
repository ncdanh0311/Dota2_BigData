#!/usr/bin/env python3
"""Build verified, versioned Gold draft statistics with Spark SQL."""
import argparse
from datetime import datetime, timezone
from functools import reduce
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import time
from urllib.parse import urlsplit
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = 'hdfs://localhost:9000/dota/processed/match_drafts_v1/'
DEFAULT_ROOT = 'hdfs://localhost:9000/dota/analytics/draft_stats_v1'
LOGIC_VERSION = 'draft-stats-v1'
KEYS = {'match_overview': ['analysis_run_id'], 'hero_stats': ['hero_id'],
        'hero_side_stats': ['hero_id', 'side'], 'hero_pair_stats': ['hero_a', 'hero_b'],
        'analysis_runs': ['analysis_run_id']}
LIMITATIONS = [
    'Descriptive historical associations, not causal synergy or predictions.',
    'No patch, timestamp, player/team identity or hero-name metadata in this input.',
    'Coverage measures processing of the selected Silver dataset only.',
    'Gold win rates must not be used as full-dataset ML train/test features.',
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def validate_input(df):
    required = {'match_id': 'bigint', 'radiant_heroes': 'array<int>',
                'dire_heroes': 'array<int>', 'radiant_win': 'int'}
    actual = {field.name: field.dataType.simpleString() for field in df.schema}
    if actual != required or len(df.columns) != 4:
        raise ValueError(f'Silver schema mismatch: expected {required}, got {actual}')
    heroes = F.concat('radiant_heroes', 'dire_heroes')
    invalid = (F.col('match_id').isNull() | (F.col('match_id') <= 0) |
               F.col('radiant_win').isNull() | ~F.col('radiant_win').isin(0, 1) |
               F.col('radiant_heroes').isNull() | F.col('dire_heroes').isNull() |
               (F.size('radiant_heroes') != 5) | (F.size('dire_heroes') != 5) |
               (F.size(F.array_distinct(heroes)) != 10) |
               F.exists(heroes, lambda value: value.isNull() | (value <= 0)))
    result = df.agg(F.count('*').alias('rows'), F.countDistinct('match_id').alias('ids'),
                    F.sum(F.when(invalid, 1).otherwise(0)).alias('invalid')).first()
    if result['rows'] == 0:
        raise ValueError('Silver input is empty.')
    if result['ids'] != result['rows'] or result['invalid']:
        raise ValueError(f'Invalid Silver: rows={result["rows"]}, '
                         f'unique_ids={result["ids"]}, invalid_rows={result["invalid"]}')
    return result['rows']


def build_tables(spark, matches, run_id, min_hero=100, min_pair=100):
    if min_hero < 1 or min_pair < 1:
        raise ValueError('Sample thresholds must be positive integers.')
    matches.select('match_id', F.array_sort('radiant_heroes').alias('radiant_heroes'),
                   F.array_sort('dire_heroes').alias('dire_heroes'), 'radiant_win') \
        .createOrReplaceTempView('matches')
    spark.sql('''
        SELECT match_id, 'radiant' AS side, radiant_heroes AS heroes,
               radiant_win AS team_win FROM matches
        UNION ALL
        SELECT match_id, 'dire' AS side, dire_heroes AS heroes,
               1 - radiant_win AS team_win FROM matches
    ''').createOrReplaceTempView('team_matches')
    spark.sql('''
        SELECT match_id, side, hero_id, team_win
        FROM team_matches LATERAL VIEW explode(heroes) AS hero_id
    ''').createOrReplaceTempView('hero_appearances')
    spark.sql('''
        SELECT match_id, side, hero_a, hero_b, team_win
        FROM team_matches
        LATERAL VIEW posexplode(heroes) a AS i, hero_a
        LATERAL VIEW posexplode(heroes) b AS j, hero_b
        WHERE i < j
    ''').createOrReplaceTempView('pair_appearances')
    spark.sql('''
        SELECT side, COUNT(*) AS side_matches, SUM(team_win) AS side_wins,
               try_divide(SUM(team_win), COUNT(*)) AS side_baseline
        FROM team_matches GROUP BY side
    ''').createOrReplaceTempView('side_baselines')
    queries = {
        'match_overview': '''
            SELECT COUNT(*) AS total_matches,
                   SUM(CAST(radiant_win AS BIGINT)) AS radiant_wins,
                   SUM(CAST(1 - radiant_win AS BIGINT)) AS dire_wins,
                   try_divide(SUM(radiant_win), COUNT(*)) AS radiant_win_rate,
                   try_divide(SUM(1 - radiant_win), COUNT(*)) AS dire_win_rate
            FROM matches''',
        'hero_stats': f'''
            SELECT hero_id, COUNT(*) AS picks, SUM(team_win) AS wins,
                   COUNT(*) - SUM(team_win) AS losses,
                   try_divide(COUNT(*), (SELECT COUNT(*) FROM matches)) AS pick_rate,
                   try_divide(SUM(team_win), COUNT(*)) AS win_rate,
                   CASE WHEN COUNT(*) >= {min_hero} THEN 'eligible'
                        ELSE 'insufficient_sample' END AS sample_status
            FROM hero_appearances GROUP BY hero_id''',
        'hero_side_stats': f'''
            SELECT h.hero_id, h.side, h.picks, h.wins, h.picks - h.wins AS losses,
                   try_divide(h.picks, b.side_matches) AS side_pick_rate,
                   try_divide(h.wins, h.picks) AS side_win_rate, b.side_baseline,
                   try_divide(h.wins, h.picks) - b.side_baseline AS delta_vs_side_baseline,
                   CASE WHEN h.picks >= {min_hero} THEN 'eligible'
                        ELSE 'insufficient_sample' END AS sample_status
            FROM (SELECT hero_id, side, COUNT(*) AS picks, SUM(team_win) AS wins
                  FROM hero_appearances GROUP BY hero_id, side) h
            JOIN side_baselines b ON h.side = b.side''',
        'hero_pair_stats': f'''
            SELECT hero_a, hero_b, COUNT(*) AS pair_matches, SUM(team_win) AS pair_wins,
                   COUNT(*) - SUM(team_win) AS pair_losses,
                   try_divide(COUNT(*), (SELECT COUNT(*) FROM matches)) AS pair_pick_rate,
                   try_divide(SUM(team_win), COUNT(*)) AS pair_win_rate,
                   CASE WHEN COUNT(*) >= {min_pair} THEN 'eligible'
                        ELSE 'insufficient_sample' END AS sample_status
            FROM pair_appearances GROUP BY hero_a, hero_b''',
    }
    return {name: spark.sql(query).withColumn('analysis_run_id', F.lit(run_id))
            .persist(StorageLevel.MEMORY_AND_DISK) for name, query in queries.items()}


def ensure(condition, message):
    if not condition:
        raise ValueError(message)


def check_keys(df, keys):
    summary = df.agg(F.count('*').alias('rows'),
                     F.countDistinct(F.struct(*keys)).alias('keys')).first()
    ensure(summary['rows'] == summary['keys'], f'Duplicate keys: {keys}')
    null_key = reduce(lambda a, b: a | b, [F.col(key).isNull() for key in keys])
    ensure(df.where(null_key).limit(1).count() == 0, f'Null keys: {keys}')
    return summary['rows']


def check_gold(spark, tables, n, min_hero, min_pair):
    counts = {name: check_keys(frame, KEYS[name]) for name, frame in tables.items()}
    ensure(counts['match_overview'] == 1, 'Overview must contain one row.')
    overview = tables['match_overview'].first().asDict()
    ensure(overview['total_matches'] == n, 'Overview match count mismatch.')
    ensure(overview['radiant_wins'] + overview['dire_wins'] == n, 'Side wins mismatch.')
    ensure(abs(overview['radiant_win_rate'] + overview['dire_win_rate'] - 1) <= 1e-9,
           'Side win rates must sum to one.')
    appearances = {}
    for view, multiplier, expected_wins in [('team_matches', 2, n),
                                            ('hero_appearances', 10, 5*n),
                                            ('pair_appearances', 20, 10*n)]:
        summary = spark.sql(f'SELECT COUNT(*) AS rows, SUM(team_win) AS wins FROM {view}').first()
        ensure(summary['rows'] == multiplier*n and summary['wins'] == expected_wins,
               f'Appearance accounting failed: {view}')
        appearances[view] = summary.asDict()
    for name, picks, wins, losses, rates, minimum in [
        ('hero_stats', 'picks', 'wins', 'losses', ['pick_rate', 'win_rate'], min_hero),
        ('hero_side_stats', 'picks', 'wins', 'losses',
         ['side_pick_rate', 'side_win_rate', 'side_baseline'], min_hero),
        ('hero_pair_stats', 'pair_matches', 'pair_wins', 'pair_losses',
         ['pair_pick_rate', 'pair_win_rate'], min_pair),
    ]:
        frame = tables[name]
        invalid = (F.col(picks).isNull() | (F.col(picks) <= 0) |
                   F.col(wins).isNull() | F.col(losses).isNull() |
                   (F.col(wins) < 0) | (F.col(losses) < 0) |
                   ((F.col(wins) + F.col(losses)) != F.col(picks)))
        for rate in rates:
            invalid = invalid | F.col(rate).isNull() | F.isnan(rate) | ~F.col(rate).between(0.0, 1.0)
        wanted = F.when(F.col(picks) >= minimum, 'eligible').otherwise('insufficient_sample')
        invalid = invalid | ~F.col('sample_status').eqNullSafe(wanted)
        ensure(frame.where(invalid).limit(1).count() == 0, f'Invalid metrics in {name}')
    hero_totals = tables['hero_stats'].agg(F.sum('picks'), F.sum('wins')).first()
    pair_totals = tables['hero_pair_stats'].agg(F.sum('pair_matches'), F.sum('pair_wins')).first()
    ensure(tuple(hero_totals) == (10*n, 5*n), 'Hero aggregate totals mismatch.')
    ensure(tuple(pair_totals) == (20*n, 10*n), 'Pair aggregate totals mismatch.')
    ensure(tables['hero_pair_stats'].where('hero_a >= hero_b').limit(1).count() == 0,
           'Pair IDs must be strictly increasing.')
    by_side = tables['hero_side_stats'].groupBy('hero_id').agg(
        F.sum('picks').alias('picks'), F.sum('wins').alias('wins'), F.sum('losses').alias('losses'))
    assert_same_table(tables['hero_stats'].select('hero_id', 'picks', 'wins', 'losses'),
                      by_side, ['hero_id'])
    delta_error = tables['hero_side_stats'].where(
        F.col('delta_vs_side_baseline').isNull() | F.isnan('delta_vs_side_baseline') |
        (F.abs(F.col('delta_vs_side_baseline') -
               (F.col('side_win_rate') - F.col('side_baseline'))) > 1e-9))
    ensure(delta_error.limit(1).count() == 0, 'Side baseline delta mismatch.')
    return {'status': 'PASS', 'row_counts': counts, 'appearances': appearances,
            'hero_picks': int(hero_totals[0]), 'hero_wins': int(hero_totals[1]),
            'pair_matches': int(pair_totals[0]), 'pair_wins': int(pair_totals[1]),
            'overview': overview}


def assert_same_table(expected, actual, keys):
    expected_types = {field.name: field.dataType.simpleString() for field in expected.schema}
    actual_types = {field.name: field.dataType.simpleString() for field in actual.schema}
    ensure(expected_types == actual_types, f'Read-back schema mismatch: {actual_types}')
    ensure(check_keys(expected, keys) == check_keys(actual, keys), 'Read-back row count mismatch.')
    left = expected.withColumn('_present', F.lit(1)).alias('a')
    right = actual.withColumn('_present', F.lit(1)).alias('b')
    condition = reduce(lambda a, b: a & b, [F.col(f'a.{key}') == F.col(f'b.{key}') for key in keys])
    joined = left.join(right, condition, 'full_outer')
    mismatch = F.col('a._present').isNull() | F.col('b._present').isNull()
    for field in expected.schema:
        a, b = F.col(f'a.{field.name}'), F.col(f'b.{field.name}')
        if isinstance(field.dataType, (T.DoubleType, T.FloatType)):
            equal = (a.isNull() & b.isNull()) | (a.isNotNull() & b.isNotNull() &
                     ~F.isnan(a) & ~F.isnan(b) & (F.abs(a-b) <= 1e-9))
        else:
            equal = a.eqNullSafe(b)
        mismatch = mismatch | ~equal
    ensure(joined.where(mismatch).limit(1).count() == 0, 'Read-back values differ by key.')


def fs_for(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def reserve_run(spark, uri):
    fs, path = fs_for(spark, uri)
    if fs.exists(path):
        raise FileExistsError(f'Run already exists; choose a new --run-id: {uri}')
    ensure(fs.mkdirs(path), f'Cannot create run directory: {uri}')
    stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(uri + '/_RUNNING'), False)
    stream.close()


def copy_json(spark, local, uri):
    fs, target = fs_for(spark, uri)
    fs.copyFromLocalFile(False, False, spark._jvm.org.apache.hadoop.fs.Path(str(local)), target)


def input_inventory(spark, uri):
    # Spark inputFiles() is best-effort and can become empty after caching.
    # Enumerate the immutable Silver directory with Hadoop FS instead.
    source_fs, source_path = fs_for(spark, uri)
    status = source_fs.getFileStatus(source_path)
    statuses = []
    if status.isFile():
        statuses.append(status)
    else:
        iterator = source_fs.listFiles(source_path, True)
        while iterator.hasNext():
            statuses.append(iterator.next())
    result = []
    for status in statuses:
        path = status.getPath()
        if not path.getName().endswith('.parquet') or any(
                part.startswith(('_', '.')) for part in PurePosixPath(path.toUri().getPath()).parts):
            continue
        checksum = source_fs.getFileChecksum(path)
        result.append({'uri': path.toString(), 'bytes': status.getLen(),
                       'modified_ms': status.getModificationTime(),
                       'hdfs_checksum': checksum.toString() if checksum is not None else None})
    ensure(result and all(item['bytes'] > 0 for item in result), 'Silver file inventory is empty or invalid.')
    return sorted(result, key=lambda item: item['uri'])


def git_metadata():
    def query(*args):
        result = subprocess.run(['git', '-C', str(PROJECT), *args], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    return {'git_commit': query('rev-parse', 'HEAD'),
            'git_dirty': query('status', '--porcelain') != '',
            'logic_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def report_samples(tables):
    heroes = tables['hero_stats']
    picked = heroes.orderBy(F.desc('picks'), F.asc('hero_id')).limit(20).collect()
    best = heroes.where("sample_status = 'eligible'").orderBy(
        F.desc('win_rate'), F.desc('picks'), F.asc('hero_id')).limit(20).collect()
    pairs = tables['hero_pair_stats'].where("sample_status = 'eligible'").orderBy(
        F.desc('pair_win_rate'), F.desc('pair_matches'), 'hero_a', 'hero_b').limit(20).collect()
    top5 = [row.hero_id for row in picked[:5]]
    side_rows = tables['hero_side_stats'].where(F.col('hero_id').isin(top5)).collect()
    comparisons = []
    for hero in top5:
        sides = {row.side: row.asDict() for row in side_rows if row.hero_id == hero}
        rad, dire = sides.get('radiant'), sides.get('dire')
        status = ('not_comparable' if not rad or not dire else 'eligible'
                  if rad['sample_status'] == dire['sample_status'] == 'eligible'
                  else 'insufficient_sample')
        comparisons.append({'hero_id': hero, 'hero_name': f'Hero {hero}',
                            'radiant': rad, 'dire': dire, 'comparison_status': status,
                            'delta_radiant_minus_dire': (rad['side_win_rate'] - dire['side_win_rate'])
                            if rad is not None and dire is not None else None})
    return {'top20_picks': [row.asDict() for row in picked],
            'top20_hero_win_rate': [row.asDict() for row in best],
            'top20_pair_win_rate': [row.asDict() for row in pairs],
            'top5_side_comparison': comparisons}


def markdown(report, samples):
    overview = report['quality_checks']['overview']
    lines = ['# Bước 3 — Phân tích đội hình bằng Spark SQL', '',
             f"Run: `{report['analysis_run_id']}` · Trạng thái: **PASS**", '',
             '| Chỉ tiêu | Kết quả |', '|---|---:|',
             f"| Tổng trận | {report['input_matches']:,} |",
             f"| Radiant thắng | {overview['radiant_wins']:,} ({overview['radiant_win_rate']:.2%}) |",
             f"| Dire thắng | {overview['dire_wins']:,} ({overview['dire_win_rate']:.2%}) |",
             f"| Hero xuất hiện | {report['quality_checks']['row_counts']['hero_stats']} |",
             f"| Cặp cùng đội xuất hiện | {report['quality_checks']['row_counts']['hero_pair_stats']:,} |",
             f"| Coverage trên Silver đầu vào | {report['coverage_pct']:.2f}% |",
             f"| Thời gian xử lý | {report['elapsed_seconds']} giây |", '']
    for title, name, is_pair, metric in [
        ('Top 20 hero theo lượt chọn', 'top20_picks', False, 'picks'),
        ('Top 20 hero theo tỷ lệ thắng, đủ mẫu', 'top20_hero_win_rate', False, 'win_rate'),
        ('Top 20 cặp theo tỷ lệ thắng, đủ mẫu', 'top20_pair_win_rate', True, 'pair_win_rate'),
    ]:
        lines += [f'## {title}', '', '| Hero / cặp | Số trận | Thắng | Pick rate | Win rate | Mẫu |',
                  '|---|---:|---:|---:|---:|---|']
        for row in samples[name]:
            prefix = 'pair_' if is_pair else ''
            label = f"Hero {row['hero_a']} + Hero {row['hero_b']}" if is_pair else f"Hero {row['hero_id']}"
            picks = row['pair_matches'] if is_pair else row['picks']
            lines.append(f"| {label} | {picks:,} | {row[prefix+'wins']:,} | "
                         f"{row[prefix+'pick_rate']:.2%} | {row[prefix+'win_rate']:.2%} | {row['sample_status']} |")
        if not samples[name]:
            lines.append('| Không có dòng đủ điều kiện | | | | | |')
        lines.append('')
    lines += ['## So sánh năm hero được chọn nhiều nhất', '',
              '| Hero | Trận Radiant | Win rate Radiant | Trận Dire | Win rate Dire | Chênh lệch (đpt) | Trạng thái |',
              '|---|---:|---:|---:|---:|---:|---|']
    for row in samples['top5_side_comparison']:
        rad, dire = row['radiant'], row['dire']
        rad_count, dire_count = rad['picks'] if rad else '—', dire['picks'] if dire else '—'
        rad_rate, dire_rate = f"{rad['side_win_rate']:.2%}" if rad else '—', f"{dire['side_win_rate']:.2%}" if dire else '—'
        delta = row['delta_radiant_minus_dire']
        delta_text = f'{delta * 100:+.2f}' if delta is not None else '—'
        lines.append(f"| {row['hero_name']} | {rad_count} | {rad_rate} | {dire_count} | {dire_rate} | {delta_text} | {row['comparison_status']} |")
    lines += ['', '## Nguồn và giới hạn', '', f"- Silver: `{report['input_uri']}`.",
              f"- Gold: `{report['output_uri']}`.",
              f"- Ngưỡng hero/cặp: {report['min_hero_matches']}/{report['min_pair_matches']} trận.",
              '- Pick rate = số trận có hero/cặp chia tổng trận, không chia tổng appearances.',
              '- Mọi bảng đã đọc lại và đối chiếu theo khóa; sai số Double tối đa 1e-9.',
              '- Tỷ lệ thắng quan sát không chứng minh phối hợp nhân quả và không phải dự đoán đội hình mới.',
              '- Chưa có patch/thời gian/tên hero; chưa làm ML, website hoặc Kafka.', '']
    return '\n'.join(lines)


def execute(spark, args):
    started, started_at = time.perf_counter(), utc_now()
    output_uri = args.output_root.rstrip('/') + '/' + args.run_id
    fs, run_path = fs_for(spark, output_uri)
    if fs.exists(run_path):
        raise FileExistsError(f'Run already exists; choose a new --run-id: {output_uri}')
    evidence = PROJECT / 'evidence' / 'step3' / args.run_id
    if evidence.exists():
        raise FileExistsError(f'Local evidence already exists: {evidence}')
    try:
        matches = spark.read.parquet(args.input).persist(StorageLevel.MEMORY_AND_DISK)
        n = validate_input(matches)
    except Exception as error:
        raise RuntimeError('Cannot read/validate Silver; start HDFS with commands/start-hadoop.ps1 '
                           f'and check --input. {error}') from error
    print(f'Input validated: {n:,} unique matches.', flush=True)
    inventory = input_inventory(spark, args.input)
    reserve_run(spark, output_uri)
    evidence.mkdir(parents=True)
    tables = {}
    try:
        tables = build_tables(spark, matches, args.run_id, args.min_hero_matches, args.min_pair_matches)
        checks = check_gold(spark, tables, n, args.min_hero_matches, args.min_pair_matches)
        if args.input.rstrip('/') == DEFAULT_INPUT.rstrip('/'):
            ensure((n, checks['overview']['radiant_wins'], checks['overview']['dire_wins']) ==
                   (90530, 46305, 44225), 'Silver v1 differs from the verified Step 2 reference.')
        print('Gold accounting verified; writing and reading back four statistics tables...', flush=True)
        readbacks = {}
        for name, frame in tables.items():
            uri = output_uri + '/' + name
            frame.coalesce(1).write.mode('errorifexists').option('compression', 'snappy').parquet(uri)
            readbacks[name] = spark.read.parquet(uri)
            assert_same_table(frame, readbacks[name], KEYS[name])
            files = [status.getPath().getName() for status in fs.listStatus(
                spark._jvm.org.apache.hadoop.fs.Path(uri)) if status.getPath().getName().endswith('.parquet')]
            ensure(len(files) == 1, f'Expected one Parquet data file in {name}.')
        samples = report_samples(readbacks)
        metadata = {'analysis_run_id': args.run_id, 'logic_version': LOGIC_VERSION,
                    'input_uri': args.input, 'output_uri': output_uri,
                    'spark_version': spark.version, **git_metadata(),
                    'started_at_utc': started_at, 'generated_at_utc': utc_now(),
                    'input_matches': n, 'processed_matches': n, 'excluded_matches': 0,
                    'coverage_pct': 100.0, 'min_hero_matches': args.min_hero_matches,
                    'min_pair_matches': args.min_pair_matches,
                    'input_files_json': json.dumps(inventory, sort_keys=True),
                    'input_fingerprint_sha256': hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest(),
                    'row_counts_json': json.dumps({**checks['row_counts'], 'analysis_runs': 1}, sort_keys=True),
                    'quality_checks_json': json.dumps(checks, sort_keys=True),
                    'limitations_json': json.dumps(LIMITATIONS),
                    'processing_elapsed_seconds': round(time.perf_counter() - started, 2),
                    'status': 'PASS'}
        meta_df = spark.createDataFrame([metadata])
        metadata_uri = output_uri + '/analysis_runs'
        meta_df.coalesce(1).write.mode('errorifexists').option('compression', 'snappy').parquet(metadata_uri)
        assert_same_table(meta_df, spark.read.parquet(metadata_uri), KEYS['analysis_runs'])
        report = {**metadata, 'input_files': inventory, 'quality_checks': checks,
                  'gold_readback_verified': True, 'elapsed_seconds': round(time.perf_counter()-started, 2)}
        for name, value in [('report.json', report), ('quality_checks.json', checks),
                             ('samples.json', samples)]:
            (evidence / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
        (evidence / 'report.md').write_text(markdown(report, samples), encoding='utf-8')
        for name in ['report.json', 'quality_checks.json', 'samples.json', 'report.md']:
            copy_json(spark, evidence / name, output_uri + '/' + name)
        running = spark._jvm.org.apache.hadoop.fs.Path(output_uri + '/_RUNNING')
        complete = spark._jvm.org.apache.hadoop.fs.Path(output_uri + '/_COMPLETED')
        ensure(fs.rename(running, complete), 'Cannot publish run completion marker.')
        print(f'PASS: {output_uri}\nReport: {evidence / "report.md"}', flush=True)
        print(json.dumps(checks, indent=2), flush=True)
    except Exception as error:
        failure = evidence / 'failure.json'
        failure.write_text(json.dumps({'status': 'FAILED', 'error': str(error),
                                      'analysis_run_id': args.run_id, 'time_utc': utc_now()}, indent=2), encoding='utf-8')
        try:
            copy_json(spark, failure, output_uri + '/_FAILED.json')
        except Exception:
            pass  # Keep the original exception, never publish a completion marker.
        raise
    finally:
        for frame in tables.values():
            frame.unpersist()
        matches.unpersist()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default=DEFAULT_INPUT)
    parser.add_argument('--output-root', default=DEFAULT_ROOT)
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
    parser.add_argument('--min-hero-matches', type=int, default=100)
    parser.add_argument('--min-pair-matches', type=int, default=100)
    args = parser.parse_args()
    root = urlsplit(args.output_root)
    if root.scheme != 'hdfs' or not root.path.startswith('/dota/analytics/') or '..' in PurePosixPath(root.path).parts:
        parser.error('--output-root must be an HDFS URI under /dota/analytics/.')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', args.run_id):
        parser.error('--run-id must be a safe single directory name (letters, digits, _ or -).')
    if min(args.min_hero_matches, args.min_pair_matches) < 1:
        parser.error('Sample thresholds must be positive integers.')
    return args


def main():
    args = arguments()
    spark = (SparkSession.builder.appName('Dota-Step3-Draft-Stats')
             .config('spark.sql.shuffle.partitions', '8').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        execute(spark, args)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
