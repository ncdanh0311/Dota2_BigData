"""Export and verify a signed-hero Logistic Regression portable bundle."""
from ml.features import FEATURE_VERSION
from ml.predict_draft import predict, validate_bundle


def make_bundle(model, vocabulary, run_id):
    if model.getStandardization() or model.getThreshold() != 0.5:
        raise ValueError('Bundle contract expects standardization=False and threshold=0.5.')
    return validate_bundle({'run_id': run_id, 'feature_version': FEATURE_VERSION,
        'hero_ids': vocabulary, 'coefficients': [float(value) for value in model.coefficients],
        'intercept': float(model.intercept), 'threshold': 0.5,
        'class_mapping': {'0': 'dire', '1': 'radiant'}})


def verify_portable(native_predictions, drafts, bundle):
    checked, max_error = 0, 0.0
    rows = native_predictions.join(drafts.select('match_id', 'radiant_heroes', 'dire_heroes'), 'match_id')
    for row in rows.toLocalIterator():
        actual = predict(bundle, row.radiant_heroes, row.dire_heroes)
        error = abs(actual['p_radiant']-row.p_radiant)
        if error > 1e-9 or actual['prediction'] != int(row.prediction):
            raise ValueError(f'Portable/native prediction mismatch: match_id={row.match_id}, error={error}')
        checked += 1
        max_error = max(max_error, error)
    if checked == 0 or checked != native_predictions.count():
        raise ValueError('Portable parity did not cover every prediction.')
    return {'status': 'PASS', 'checked_rows': checked, 'max_probability_error': max_error,
            'probability_tolerance': 1e-9, 'predicted_labels_match': True}
