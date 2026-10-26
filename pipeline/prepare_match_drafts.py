#!/usr/bin/env python3
"""Validate Zenodo v3 snapshots and publish one pre-match draft per match."""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = 'hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv'
DEFAULT_OUTPUT = 'hdfs://localhost:9000/dota/processed/match_drafts_v1'
HEROES = [f'{side}_h{i}' for side in ('rad', 'dire') for i in range(1, 6)]
COLUMNS = [
    'match_id', 'minute', 'radiant_win', 'duration_min', *HEROES,
    'rad_kills', 'dire_kills', 'kill_diff', 'kill_ratio', 'nw_adv',
    'rad_core1_gold', 'rad_core2_gold', 'rad_core3_gold',
    'dire_core1_gold', 'dire_core2_gold', 'dire_core3_gold',
    'core1_nw_diff', 'core2_nw_diff', 'core3_nw_diff', 'rad_sup_gold',
    'dire_sup_gold', 'rad_core_share', 'dire_core_share', 'rad_total_nw',
    'dire_total_nw', 'item_cat_barrier_adv', 'item_cat_control_adv',
    'item_cat_damage_adv', 'item_cat_durability_adv', 'item_cat_farming_adv',
    'item_cat_healing_adv', 'item_cat_initiation_adv', 'item_cat_magic_damage_adv',
    'item_cat_refresher_adv', 'item_cat_save_adv', 'item_count_adv',
    'bkb_count_adv', 'rad_bkb_count', 'dire_bkb_count',
]
REQUIRED = ['match_id', 'minute', 'radiant_win', *HEROES]
INTS = set(['minute', 'radiant_win', *HEROES, 'rad_kills', 'dire_kills',
            'kill_diff', 'item_count_adv', 'bkb_count_adv', 'rad_bkb_count', 'dire_bkb_count'])
SCHEMA = T.StructType([
    T.StructField(name, T.LongType() if name == 'match_id' else
                  T.IntegerType() if name in INTS else T.DoubleType(), True)
    for name in COLUMNS
] + [T.StructField('_corrupt_record', T.StringType(), True)])


def reasons(*conditions):
    return F.filter(F.array(*[F.when(test, F.lit(code)) for test, code in conditions]),
                    lambda value: value.isNotNull())


def parse_lines(lines):
    """Input: one numeric CSV record per row in `_raw_line`; no header."""
    parsed = lines.withColumn('_parsed', F.from_csv('_raw_line', SCHEMA.toDDL(), {
        'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record',
        'ignoreLeadingWhiteSpace': 'true', 'ignoreTrailingWhiteSpace': 'true',
    })).select('_raw_line', '_parsed.*')
    rad = F.array_sort(F.array(*[F.col(f'rad_h{i}') for i in range(1, 6)]))
    dire = F.array_sort(F.array(*[F.col(f'dire_h{i}') for i in range(1, 6)]))
    parsed = parsed.withColumn('radiant_heroes', rad).withColumn('dire_heroes', dire)
    all_heroes = F.concat('radiant_heroes', 'dire_heroes')
    return parsed.withColumn('row_reasons', reasons(
        ((F.col('_corrupt_record').isNotNull() |
          (F.size(F.split('_raw_line', ',', -1)) != len(COLUMNS))), 'MALFORMED_CSV'),
        (F.col('match_id').isNull() | (F.col('match_id') <= 0), 'INVALID_MATCH_ID'),
        (F.col('minute').isNull() | (F.col('minute') < 0), 'INVALID_MINUTE'),
        (F.col('radiant_win').isNull() | ~F.col('radiant_win').isin(0, 1), 'INVALID_LABEL'),
        (F.exists(all_heroes, lambda hero: hero.isNull() | (hero <= 0)), 'INVALID_HERO_ID'),
        (F.size(F.array_distinct(all_heroes)) != 10, 'DUPLICATE_HERO'),
    ))


def classify(parsed):
    """Never choose an arbitrary snapshot from a conflicting match."""
    unique = parsed.dropDuplicates(['_raw_line']).persist(StorageLevel.DISK_ONLY)
    identified = unique.where(F.col('match_id').isNotNull())
    conflicting_keys = (identified.groupBy('match_id', 'minute').count()
                        .where('count > 1').select('match_id').distinct()
                        .withColumn('_key_conflict', F.lit(True)))
    matches = (identified.groupBy('match_id').agg(
        F.count('*').alias('snapshot_count'),
        F.sum((F.size('row_reasons') > 0).cast('long')).alias('invalid_snapshots'),
        F.countDistinct(F.struct('radiant_heroes', 'dire_heroes')).alias('draft_versions'),
        F.countDistinct('radiant_win').alias('label_versions'),
        F.first('radiant_heroes').alias('radiant_heroes'),
        F.first('dire_heroes').alias('dire_heroes'),
        F.first('radiant_win').alias('radiant_win'),
    ).join(conflicting_keys, 'match_id', 'left')
        .withColumn('match_reasons', reasons(
            (F.col('invalid_snapshots') > 0, 'HAS_INVALID_SNAPSHOT'),
            (F.coalesce(F.col('_key_conflict'), F.lit(False)), 'CONFLICTING_SNAPSHOT_KEY'),
            (F.col('draft_versions') > 1, 'CONFLICTING_DRAFT'),
            (F.col('label_versions') > 1, 'CONFLICTING_LABEL'),
        )).persist(StorageLevel.MEMORY_AND_DISK))
    good = matches.where(F.size('match_reasons') == 0).select(
        'match_id', 'radiant_heroes', 'dire_heroes', 'radiant_win')
    bad = matches.where(F.size('match_reasons') > 0)
    quarantined = (unique.join(bad.select('match_id', 'match_reasons'), 'match_id', 'left')
                   .where(F.col('match_id').isNull() | F.col('match_reasons').isNotNull())
                   .withColumn('match_reasons', F.coalesce(
                       'match_reasons', F.array(F.lit('UNIDENTIFIABLE_MATCH')))))
    return unique, matches, good, bad, quarantined


def reason_counts(df, column):
    return {row['reason']: row['count'] for row in
            df.select(F.explode(column).alias('reason')).groupBy('reason').count().collect()}


def validate_output(df, expected_count):
    expected_types = {'match_id': T.LongType(), 'radiant_heroes': T.ArrayType(T.IntegerType()),
                      'dire_heroes': T.ArrayType(T.IntegerType()), 'radiant_win': T.IntegerType()}
    if df.columns != list(expected_types):
        raise RuntimeError(f'Unexpected Parquet columns: {df.columns}')
    for field in df.schema:
        # Parquet can change nullability; compare element types, not nullability flags.
        if field.dataType != expected_types[field.name]:
            raise RuntimeError(f'Unexpected Parquet type: {field}')
    summary = df.agg(F.count('*').alias('rows'),
                     F.countDistinct('match_id').alias('unique_ids')).first()
    if summary.rows != expected_count or summary.unique_ids != expected_count:
        raise RuntimeError('Parquet row count / unique match_id check failed.')
    heroes = F.concat('radiant_heroes', 'dire_heroes')
    invalid = df.where(
        F.col('match_id').isNull() | (F.col('match_id') <= 0) |
        F.col('radiant_win').isNull() | ~F.col('radiant_win').isin(0, 1) |
        F.col('radiant_heroes').isNull() | F.col('dire_heroes').isNull() |
        (F.size('radiant_heroes') != 5) | (F.size('dire_heroes') != 5) |
        (F.size(F.array_distinct(heroes)) != 10) |
        F.exists(heroes, lambda hero: hero.isNull() | (hero <= 0)) |
        (F.col('radiant_heroes') != F.array_sort('radiant_heroes')) |
        (F.col('dire_heroes') != F.array_sort('dire_heroes')))
    if invalid.limit(1).count():
        raise RuntimeError('Invalid draft in read-back Parquet.')


def fs_for(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def read_header(spark, uri):
    fs, path = fs_for(spark, uri)
    try:
        if not fs.isFile(path):
            raise RuntimeError(f'Input CSV does not exist: {uri}')
        stream = fs.open(path)
        reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(stream, 'UTF-8'))
        try:
            return reader.readLine()
        finally:
            reader.close()
    except Exception as error:
        raise RuntimeError('Cannot read input on HDFS. Start HDFS with start-hadoop.ps1 '
                           f'and check the input path. Details: {error}') from error


def markdown(report):
    metrics = report['metrics']
    lines = ['# Báo cáo chất lượng dữ liệu — Bước 2', '',
             f"Trạng thái: **{report['status']}**", '',
             '| Chỉ tiêu | Kết quả |', '|---|---:|']
    for key in ['source_rows', 'identified_matches', 'exact_duplicate_rows',
                'invalid_source_rows', 'conflicting_matches', 'valid_matches',
                'quarantined_matches', 'unidentifiable_source_rows', 'quarantined_unique_rows']:
        lines.append(f'| {key} | {metrics[key]:,} |')
    lines += ['', f"Nhãn trên bảng trận hợp lệ: `{report['label_distribution']}`", '',
              f"Thời gian: {report['elapsed_seconds']} giây.", '',
              f"Đầu vào: `{report['input']}`", '', f"Đầu ra: `{report['output']}`", '',
              'Đã đọc lại Parquet, kiểm tra kiểu dữ liệu, khóa duy nhất và 10 hero hợp lệ.',
              'Bảng đầu ra chỉ có match_id, radiant_heroes, dire_heroes và radiant_win.',
              'Không có patch/start_time trong nguồn; chưa chia tập và chưa huấn luyện ML.', '']
    if report['reference_differences']:
        lines += ['Cảnh báo khác manifest nguồn:', *report['reference_differences'], '']
    return '\n'.join(lines)


def execute(spark, args):
    started = time.perf_counter()
    manifest = json.loads((PROJECT / 'data/reference/source_manifest.json').read_text(encoding='utf-8'))
    if manifest['columns'] != COLUMNS:
        raise RuntimeError('Source manifest does not match the fixed schema.')
    header = read_header(spark, args.input)
    if header is None or next(csv.reader([header.lstrip('\ufeff')])) != COLUMNS:
        raise RuntimeError('CSV header does not match the 48 columns in the source manifest.')
    output_fs, output_path = fs_for(spark, args.output)
    quarantine = args.quarantine
    report_uri = args.report_hdfs
    for uri in [args.output, quarantine, report_uri]:
        fs, path = fs_for(spark, uri)
        if fs.exists(path):
            raise RuntimeError(f'Output already exists; use a new --output path: {uri}')
    raw = spark.read.text(args.input).select(F.col('value').alias('_raw_line'))
    # Source is numeric, one record per line; header cannot be a valid numeric row.
    data = raw.where(F.col('_raw_line') != header)
    parsed = parse_lines(data).persist(StorageLevel.DISK_ONLY)
    print('Reading and validating all source snapshots...', flush=True)
    source_rows = parsed.count()  # Materialize before querying corrupt-record fields.
    print(f'Read {source_rows:,} source rows. Checking nulls and match consistency...', flush=True)
    if source_rows == 0:
        raise RuntimeError('Input has no data rows.')
    nulls = parsed.agg(*[F.sum(F.col(c).isNull().cast('long')).alias(c) for c in COLUMNS]).first().asDict()
    invalid_rows = parsed.where(F.size('row_reasons') > 0).count()
    unidentifiable = parsed.where(F.col('match_id').isNull()).count()
    unique, matches, good, bad, quarantine_df = classify(parsed)
    unique_count = unique.count()
    identified_count = matches.count()
    good_count = good.count()
    bad_count = bad.count()
    quarantine_count = quarantine_df.count()
    conflict_count = bad.where(F.exists('match_reasons', lambda code: code.startswith('CONFLICTING_'))).count()
    if good_count == 0 or good_count + bad_count != identified_count:
        raise RuntimeError('Empty valid output or match accounting mismatch.')
    labels = {str(row.radiant_win): row['count'] for row in good.groupBy('radiant_win').count().collect()}
    row_reason_counts = reason_counts(parsed, 'row_reasons')
    match_reason_counts = reason_counts(bad, 'match_reasons')
    print(f'Validated {identified_count:,} matches; accepted {good_count:,}, rejected {bad_count:,}.', flush=True)
    good.repartition(2).write.mode('errorifexists').option('compression', 'snappy').parquet(args.output)
    readback = spark.read.parquet(args.output)
    validate_output(readback, good_count)
    files = [status.getPath().getName() for status in output_fs.listStatus(output_path)
             if status.getPath().getName().endswith('.parquet')]
    if len(files) != 2:
        raise RuntimeError(f'Expected two Parquet data files, found {len(files)}.')
    if quarantine_count:
        quarantine_df.repartition(2).write.mode('errorifexists').option('compression', 'snappy').parquet(quarantine)
    differences = []
    if source_rows != manifest['data_rows']:
        differences.append(f"source_rows={source_rows}, manifest={manifest['data_rows']}")
    if identified_count != 90530:
        differences.append(f'identified_matches={identified_count}, reference=90530')
    report = {
        'status': 'PASS', 'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'spark_version': spark.version, 'input': args.input, 'output': args.output,
        'quarantine_output': quarantine if quarantine_count else None,
        'hdfs_report': report_uri, 'metrics': {
            'source_rows': source_rows, 'identified_matches': identified_count,
            'unique_snapshot_rows': unique_count, 'exact_duplicate_rows': source_rows - unique_count,
            'invalid_source_rows': invalid_rows, 'conflicting_matches': conflict_count,
            'valid_matches': good_count, 'quarantined_matches': bad_count,
            'unidentifiable_source_rows': unidentifiable, 'quarantined_unique_rows': quarantine_count,
        }, 'missing_values_all_columns': nulls,
        'missing_values_required_columns': {c: nulls[c] for c in REQUIRED},
        'row_reason_counts': row_reason_counts, 'match_reason_counts': match_reason_counts,
        'label_distribution': labels, 'parquet_files': sorted(files),
        'parquet_schema': readback.schema.jsonValue(), 'parquet_readback_verified': True,
        'source_manifest_md5': manifest['md5'], 'reference_differences': differences,
        'elapsed_seconds': round(time.perf_counter() - started, 2),
    }
    evidence = args.evidence_dir
    evidence.mkdir(parents=True, exist_ok=True)
    report_file = evidence / 'report.json'
    report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (evidence / 'report.md').write_text(markdown(report), encoding='utf-8')
    (evidence / 'sample_matches.json').write_text(
        json.dumps([row.asDict() for row in readback.orderBy('match_id').limit(10).collect()],
                   ensure_ascii=False, indent=2), encoding='utf-8')
    report_fs, report_path = fs_for(spark, report_uri)
    report_fs.mkdirs(report_path.getParent())
    report_fs.copyFromLocalFile(False, False, spark._jvm.org.apache.hadoop.fs.Path(str(report_file)), report_path)
    for frame in [matches, unique, parsed]:
        frame.unpersist()
    print(json.dumps(report['metrics'], indent=2), flush=True)
    print(f'PASS: Parquet read-back verified. Report: {report_file}', flush=True)


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default=DEFAULT_INPUT)
    parser.add_argument('--output', default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = urlsplit(args.output)
    if output.scheme != 'hdfs' or not output.path.startswith('/dota/processed/'):
        parser.error('--output must be an HDFS URI under /dota/processed/.')
    name = output.path.rstrip('/').rsplit('/', 1)[-1]
    if not name or name == 'quarantine':
        parser.error('Choose a named output dataset.')
    base = f'hdfs://{output.netloc}'
    args.quarantine = base + '/dota/processed/quarantine/' + name
    args.report_hdfs = base + '/dota/analytics/data_quality/' + name + '.json'
    args.evidence_dir = PROJECT / 'evidence' / 'step2' if args.output == DEFAULT_OUTPUT else PROJECT / 'evidence' / 'step2' / name
    return args


def main():
    args = arguments()
    spark = (SparkSession.builder.appName('Dota-Step2-Match-Drafts')
             .config('spark.sql.shuffle.partitions', '8')
             .config('spark.sql.files.maxPartitionBytes', str(16 * 1024 * 1024))
             .config('spark.sql.inMemoryColumnarStorage.batchSize', '1000')
             .config('spark.sql.csv.parser.columnPruning.enabled', 'false')
             .config('spark.sql.parquet.compression.codec', 'snappy').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        execute(spark, args)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'STEP 2 FAILED: {error}', file=sys.stderr, flush=True)
        raise
