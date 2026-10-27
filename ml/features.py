"""Deterministic match splits and a train-only signed hero vocabulary."""
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from analytics.draft_stats import validate_input

SPLIT_RULE = 'int(sha256(str(seed)+":"+str(match_id))[:8],16)%10000: <7000 train, <8500 validation, else test'
FEATURE_VERSION = 'signed-hero-v1'


def assign_splits(matches, seed=42):
    digest = F.sha2(F.concat(F.lit(str(seed) + ':'), F.col('match_id').cast('string')), 256)
    return (matches.withColumn('bucket', F.pmod(F.conv(F.substring(digest, 1, 8), 16, 10).cast('long'), F.lit(10000)))
            .withColumn('split', F.when(F.col('bucket') < 7000, 'train')
                        .when(F.col('bucket') < 8500, 'validation').otherwise('test')))


def validate_splits(assigned, expected_rows):
    rows = assigned.groupBy('split', 'radiant_win').count().collect()
    counts = {name: {'0': 0, '1': 0} for name in ['train', 'validation', 'test']}
    for row in rows:
        if row.split not in counts or row.radiant_win not in (0, 1):
            raise ValueError('Unexpected split or label.')
        counts[row.split][str(row.radiant_win)] = row['count']
    if sum(sum(values.values()) for values in counts.values()) != expected_rows:
        raise ValueError('Split accounting does not cover all source matches.')
    if any(min(values.values()) == 0 for values in counts.values()):
        raise ValueError('Every split must contain both classes.')
    if assigned.select('match_id').distinct().count() != expected_rows:
        raise ValueError('Duplicate match_id / overlap between splits.')
    return counts


def fit_vocabulary(assigned):
    train = assigned.where("split = 'train'")
    vocabulary = [row.hero_id for row in train.select(
        F.explode(F.concat('radiant_heroes', 'dire_heroes')).alias('hero_id'))
        .distinct().orderBy('hero_id').collect()]
    if not vocabulary:
        raise ValueError('Training vocabulary is empty.')
    return vocabulary


def reject_unknown_heroes(matches, vocabulary):
    allowed = F.array(*[F.lit(hero).cast('int') for hero in vocabulary])
    unknown = (matches.select(F.explode(F.array_except(
        F.concat('radiant_heroes', 'dire_heroes'), allowed)).alias('hero_id'))
        .distinct().orderBy('hero_id').collect())
    if unknown:
        raise ValueError(f'Heroes absent from training vocabulary: {[row.hero_id for row in unknown]}')


def encode_features(matches, vocabulary):
    if not vocabulary or vocabulary != sorted(set(vocabulary)):
        raise ValueError('Vocabulary must be nonempty, sorted and unique.')
    reject_unknown_heroes(matches, vocabulary)
    columns = [f'_hero_{index}' for index in range(len(vocabulary))]
    projected = matches.select('*', *[
        F.when(F.array_contains('radiant_heroes', hero), F.lit(1.0))
         .when(F.array_contains('dire_heroes', hero), F.lit(-1.0))
         .otherwise(F.lit(0.0)).alias(column)
        for hero, column in zip(vocabulary, columns)
    ])
    result = VectorAssembler(inputCols=columns, outputCol='features').transform(projected)
    return result.drop(*columns).withColumn('label', F.col('radiant_win').cast('double'))


def validate_features(encoded, dimension):
    values = vector_to_array('features')
    invalid = encoded.where((F.size(values) != dimension) |
        (F.size(F.filter(values, lambda value: value != 0)) != 10) |
        F.exists(values, lambda value: ~value.isin(-1.0, 0.0, 1.0)))
    if invalid.limit(1).count():
        raise ValueError('Encoded vector must have the expected width and ten signed heroes.')
