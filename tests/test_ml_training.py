#!/usr/bin/env python3
import json
import math
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession, functions as F
from analytics.draft_stats import assert_same_table, fs_for, reserve_run
from ml.features import assign_splits, encode_features, fit_vocabulary, validate_splits
from ml.training import baseline_scored, evaluate, scored, select_candidate, train_candidates


def fixture(spark):
    rows = []
    for i in range(1, 241):
        heroes = [((i+j) % 12)+1 for j in range(10)]
        rows.append((i, heroes[:5], heroes[5:], int(1 in heroes[:5])))
    return spark.createDataFrame(rows, 'match_id LONG, radiant_heroes ARRAY<INT>, dire_heroes ARRAY<INT>, radiant_win INT')


def main():
    spark = (SparkSession.builder.master('local[2]').appName('Dota-ML-Training-Tests')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        manual = spark.createDataFrame([(1, 0., .1, 0.), (2, 1., .8, 1.),
                                       (3, 1., .4, 0.), (4, 0., .7, 1.)],
                                      'match_id LONG, label DOUBLE, p_radiant DOUBLE, prediction DOUBLE')
        metrics = evaluate(manual)
        assert metrics['confusion_matrix'] == {'tn': 1, 'fp': 1, 'fn': 1, 'tp': 1}
        assert metrics['accuracy'] == metrics['precision_radiant'] == metrics['recall_radiant'] == metrics['f1_radiant'] == .5
        assert abs(metrics['roc_auc']-.75) < 1e-12
        expected_loss = -sum(math.log(p) for p in [.9, .8, .4, .3])/4
        assert abs(metrics['log_loss']-expected_loss) < 1e-12
        extreme = manual.withColumn('p_radiant', F.when(F.col('label') == 1, 0.).otherwise(1.))
        assert math.isfinite(evaluate(extreme)['log_loss'])
        # All candidate selection inputs are validation metrics, never test results.
        candidates = [{'reg_param': reg, 'validation': {'log_loss': loss, 'roc_auc': auc}}
                      for reg, loss, auc in [(0., .5, .6), (.01, .5+5e-13, .7), (.1, .5+8e-13, .7)]]
        assert select_candidate(candidates)['reg_param'] == .1
        assert select_candidate([*candidates, {'reg_param': .2, 'validation': {'log_loss': .4, 'roc_auc': .1}}])['reg_param'] == .2
        assigned = assign_splits(fixture(spark)).cache()
        counts = validate_splits(assigned, 240)
        vocabulary = fit_vocabulary(assigned)
        encoded = encode_features(assigned, vocabulary).cache()
        data = {name: encoded.where(F.col('split') == name) for name in ['train', 'validation', 'test']}
        prior = counts['train']['1']/sum(counts['train'].values())
        baseline = baseline_scored(data['validation'], prior)
        assert baseline.select('p_radiant').distinct().first().p_radiant == prior
        assert baseline.select('prediction').distinct().first().prediction == float(prior > .5)
        assert baseline_scored(data['validation'], .5).first().prediction == 0.
        model, selected, summaries = train_candidates(data['train'], data['validation'])
        assert len(summaries) == 3 and selected['reg_param'] == select_candidate(summaries)['reg_param']
        test = scored(model, data['test']).cache()
        evaluated = evaluate(test)
        with tempfile.TemporaryDirectory(prefix='dota-ml-model-') as folder:
            uri = Path(folder, 'model').as_uri()
            model.write().save(uri)
            reloaded = LogisticRegressionModel.load(uri)
            assert_same_table(test, scored(reloaded, data['test']), ['match_id'])
            run = Path(folder, 'reserved').as_uri()
            reserve_run(spark, run)
            try:
                reserve_run(spark, run)
            except FileExistsError:
                pass
            else:
                raise AssertionError('Existing ML run must not be overwritten.')
            assert not Path(folder, 'reserved/_COMPLETED').exists()
        result = {'status': 'PASS', 'fixture_matches': 240, 'splits': counts,
                  'selected_reg_param': selected['reg_param'], 'test_metrics': evaluated,
                  'scenarios': ['hand-computed confusion/F1/AUC/log loss', 'clipped extreme log loss',
                                'train-prior baseline and tie', 'validation-only selection and tie-breaks',
                                'three sequential candidates', 'native model save/load parity',
                                'run overwrite blocked', 'unfinished run has no completion marker']}
        dest = Path(__file__).resolve().parents[1]/'evidence/step4/training_tests.json'
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2), flush=True)
        test.unpersist(); encoded.unpersist(); assigned.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
