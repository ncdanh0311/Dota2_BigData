# Bước 4 — Logistic Regression dự đoán đội hình

Run: `af-0f492d72a8741ec5-train_lr-t1-90851e0d` · **PASS**

Nguồn: `hdfs://localhost:9000/dota/processed/match_drafts_af-0f492d72a8741ec5-normalize_silver-t1-2c37d1c5` · Seed: 42 · Feature: 127 hero.

| Tập | Dire thắng | Radiant thắng | Tổng |
|---|---:|---:|---:|
| train | 31,009 | 32,139 | 63,148 |
| validation | 6,503 | 7,115 | 13,618 |
| test | 6,713 | 7,051 | 13,764 |

## Validation — chọn cấu hình

| regParam | Accuracy | ROC-AUC | Log loss | Vòng lặp | Solver |
|---:|---:|---:|---:|---:|---|
| 0.0 | 0.5429 | 0.5570 | 0.687358 | 10 | stopped_before_max_iter |
| 0.01 | 0.5438 | 0.5528 | 0.688422 | 16 | stopped_before_max_iter |
| 0.1 | 0.5381 | 0.5472 | 0.691170 | 22 | stopped_before_max_iter |

Chọn regParam=0.0 theo validation log loss; không refit train+validation.

## Test — đánh giá sau khi chốt mô hình

| Mô hình | Accuracy | Precision (Radiant) | Recall (Radiant) | F1 (Radiant) | ROC-AUC | Log loss |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 0.5123 | 0.5123 | 1.0000 | 0.6775 | 0.5000 | 0.692868 |
| logistic_regression | 0.5449 | 0.5508 | 0.6053 | 0.5768 | 0.5589 | 0.687606 |

Confusion matrix (TN, FP, FN, TP):

```json
{
  "baseline": {
    "tn": 0,
    "fp": 6713,
    "fn": 0,
    "tp": 7051
  },
  "logistic_regression": {
    "tn": 3232,
    "fp": 3481,
    "fn": 2783,
    "tp": 4268
  }
}
```

## Kiểm chứng và giới hạn

- Mapping chỉ fit trên train; holdout không chứa hero ngoài mapping.
- Chọn tham số bằng validation; test không dùng để tuning hoặc đổi threshold.
- Chia theo hash match ID, không phải theo thời gian/patch.
- Baseline dùng prior Radiant chỉ từ train; tỷ lệ Gold không tham gia feature.
- Số liệu là holdout trong cùng dataset, không bảo đảm hiệu quả meta hiện tại.
- So sánh baseline: `{'accuracy_delta': 0.03262133100842779, 'log_loss_improvement': 0.0052620107588373655}`.
- Thời gian xử lý: 69.28 giây.
