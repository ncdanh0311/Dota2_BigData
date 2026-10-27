#!/usr/bin/env python3
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession, functions as F
from ml.export import make_bundle, verify_portable
from ml.features import assign_splits, encode_features, fit_vocabulary
from ml.predict_draft import predict, stable_sigmoid, validate_bundle
from ml.training import FIXED_PARAMS, scored
from test_ml_training import fixture


def rejected(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError('Expected rejected draft/bundle.')


def main():
    simple = {'run_id': 'fixture', 'feature_version': 'signed-hero-v1', 'hero_ids': list(range(1, 11)),
              'coefficients': [0.0]*10, 'intercept': 0.0, 'threshold': .5,
              'class_mapping': {'0': 'dire', '1': 'radiant'}}
    rad, dire = list(range(1, 6)), list(range(6, 11))
    tied = predict(simple, rad, dire)
    assert tied['p_radiant'] == tied['p_dire'] == .5 and tied['prediction'] == 0
    assert stable_sigmoid(1000) == 1.0 and stable_sigmoid(-1000) == 0.0
    assert predict({**simple, 'intercept': 1000.}, rad, dire)['p_radiant'] == 1.0
    assert predict({**simple, 'intercept': -1000.}, rad, dire)['p_radiant'] == 0.0
    nontrivial = {**simple, 'intercept': .3, 'coefficients': [float(i)/10 for i in range(10)]}
    original = predict(nontrivial, rad, dire)
    assert original == predict(nontrivial, list(reversed(rad)), list(reversed(dire)))
    swapped = predict(nontrivial, dire, rad)
    assert not math.isclose(original['p_radiant'], 1-swapped['p_radiant'])  # intercept represents side bias
    for bad_rad, bad_dire in [(rad[:4], dire), ([1, 1, 3, 4, 5], dire),
                              (rad, [1, 7, 8, 9, 10]), ([99, 2, 3, 4, 5], dire),
                              ([True, 2, 3, 4, 5], dire), ([0, 2, 3, 4, 5], dire)]:
        rejected(lambda a=bad_rad, b=bad_dire: predict(simple, a, b))
    rejected(lambda: validate_bundle({**simple, 'coefficients': [float('nan')]*10}))
    rejected(lambda: validate_bundle({**simple, 'feature_version': 'wrong'}))
    with tempfile.TemporaryDirectory(prefix='dota-portable-cli-') as folder:
        bundle_path = Path(folder)/'bundle.json'
        bundle_path.write_text(json.dumps(nontrivial), encoding='utf-8')
        # -S removes site packages, proving inference has no Spark/numpy dependency.
        result = subprocess.run([sys.executable, '-S', '-m', 'ml.predict_draft', '--bundle', str(bundle_path),
                                 '--radiant', '1,2,3,4,5', '--dire', '6,7,8,9,10'],
                                cwd=ROOT, capture_output=True, text=True, check=True)
        assert json.loads(result.stdout) == original
    spark = (SparkSession.builder.master('local[2]').appName('Dota-ML-Inference-Tests')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        assigned = assign_splits(fixture(spark))
        vocabulary = fit_vocabulary(assigned)
        encoded = encode_features(assigned, vocabulary).cache()
        model = LogisticRegression(regParam=.01, **FIXED_PARAMS).fit(encoded.where("split='train'"))
        bundle = make_bundle(model, vocabulary, 'fixture-model')
        parity = verify_portable(scored(model, encoded), encoded, bundle)
        assert parity['checked_rows'] == 240
        # Construct a native zero-coefficient model to verify the exact 0.5 tie rule.
        balanced = encoded.withColumn('label', (F.col('match_id') % 2).cast('double'))
        constant = balanced.withColumn('features', F.array(*[F.lit(0.0) for _ in vocabulary]))
        from pyspark.ml.functions import array_to_vector
        constant = constant.withColumn('features', array_to_vector('features'))
        tie_model = LogisticRegression(regParam=.01, **FIXED_PARAMS).fit(constant)
        tie_bundle = make_bundle(tie_model, vocabulary, 'fixture-tie')
        tie_probability = float(tie_model.transform(constant).first().probability[1])
        assert abs(tie_probability-.5) < 1e-12
        assert predict(tie_bundle, encoded.first().radiant_heroes, encoded.first().dire_heroes)['prediction'] == int(tie_model.transform(constant).first().prediction)
        result = {'status': 'PASS', 'spark_bundle_parity': parity,
                  'scenarios': ['standard-library-only CLI (-S)', 'native probability/label parity',
                                'threshold 0.5 parity', 'stable extreme sigmoid', 'permutation invariance',
                                'side bias retained', 'invalid/unknown/duplicate heroes rejected',
                                'invalid bundles rejected']}
        dest = ROOT/'evidence/step4/inference_tests.json'
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2), flush=True)
        encoded.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
