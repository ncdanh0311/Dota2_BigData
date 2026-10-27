"""Sequential candidate fitting and binary metrics; test is never used for selection."""
import math

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

REG_PARAMS = [0.0, 0.01, 0.1]
FIXED_PARAMS = {'elasticNetParam': 0.0, 'maxIter': 100, 'tol': 1e-6,
                'fitIntercept': True, 'standardization': False, 'threshold': 0.5}


def scored(model, data):
    return model.transform(data).select('match_id', 'label',
        vector_to_array('probability')[1].alias('p_radiant'), 'prediction')


def baseline_scored(data, probability):
    if not 0.0 < probability < 1.0:
        raise ValueError('Training prior must contain both classes.')
    return data.select('match_id', 'label', F.lit(float(probability)).alias('p_radiant'),
                       F.lit(float(probability > 0.5)).alias('prediction'))


def evaluate(predictions):
    bad = predictions.where(F.col('label').isNull() | ~F.col('label').isin(0.0, 1.0) |
        F.col('prediction').isNull() | ~F.col('prediction').isin(0.0, 1.0) |
        F.col('p_radiant').isNull() | F.isnan('p_radiant') | ~F.col('p_radiant').between(0.0, 1.0))
    if bad.limit(1).count():
        raise ValueError('Invalid probability, prediction or label.')
    p = F.least(F.lit(1-1e-15), F.greatest(F.lit(1e-15), F.col('p_radiant')))
    counts = predictions.agg(F.count('*').alias('rows'), F.countDistinct('label').alias('classes'),
        *[F.sum(F.when((F.col('label') == truth) & (F.col('prediction') == prediction), 1)
                .otherwise(0)).alias(name)
          for name, truth, prediction in [('tn', 0, 0), ('fp', 0, 1), ('fn', 1, 0), ('tp', 1, 1)]],
        F.avg(-(F.col('label')*F.log(p) + (1-F.col('label'))*F.log(1-p))).alias('log_loss')).first().asDict()
    if counts['rows'] == 0 or counts['classes'] != 2:
        raise ValueError('Binary evaluation requires nonempty data with both classes.')
    tn, fp, fn, tp = [counts[name] for name in ['tn', 'fp', 'fn', 'tp']]
    precision = tp/(tp+fp) if tp+fp else 0.0
    recall = tp/(tp+fn) if tp+fn else 0.0
    auc = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='p_radiant',
              metricName='areaUnderROC', numBins=0).evaluate(predictions)
    result = {'rows': counts['rows'], 'accuracy': (tp+tn)/counts['rows'],
              'precision_radiant': precision, 'recall_radiant': recall,
              'f1_radiant': 2*precision*recall/(precision+recall) if precision+recall else 0.0,
              'roc_auc': auc, 'log_loss': counts['log_loss'],
              'confusion_matrix': {'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp}}
    if not all(math.isfinite(result[key]) for key in ['accuracy', 'precision_radiant',
        'recall_radiant', 'f1_radiant', 'roc_auc', 'log_loss']):
        raise ValueError('Nonfinite evaluation metric.')
    return result


def select_candidate(candidates):
    if not candidates:
        raise ValueError('No candidate to select.')
    best = candidates[0]
    for item in candidates[1:]:
        loss, incumbent = item['validation']['log_loss'], best['validation']['log_loss']
        if loss < incumbent - 1e-12 or (abs(loss-incumbent) <= 1e-12 and
            (item['validation']['roc_auc'] > best['validation']['roc_auc'] or
             (item['validation']['roc_auc'] == best['validation']['roc_auc'] and
              item['reg_param'] > best['reg_param']))):
            best = item
    return best


def train_candidates(train, validation):
    candidates, models = [], {}
    for reg in REG_PARAMS:
        print(f'Fitting Logistic Regression regParam={reg} on train only...', flush=True)
        model = LogisticRegression(regParam=reg, **FIXED_PARAMS).fit(train)
        if not all(math.isfinite(float(value)) for value in [*model.coefficients, model.intercept]):
            raise ValueError('Nonfinite model coefficients.')
        predictions = scored(model, validation).cache()
        try:
            metrics = evaluate(predictions)
        finally:
            predictions.unpersist()
        iterations = int(model.summary.totalIterations)
        objective = [float(value) for value in model.summary.objectiveHistory]
        candidates.append({'reg_param': reg, 'validation': metrics, 'iterations': iterations,
                           'solver_status': 'max_iter_reached' if iterations >= FIXED_PARAMS['maxIter'] else 'stopped_before_max_iter',
                           'objective_history': objective})
        models[reg] = model
    selected = select_candidate(candidates)
    return models[selected['reg_param']], selected, candidates
