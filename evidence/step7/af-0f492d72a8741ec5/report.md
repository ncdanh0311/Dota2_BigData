# Airflow batch — PASS

DAG: `dota_batch_pipeline`

Run: `step7-batch-real-v1`

```json
{
  "snapshot_id": "af-0f492d72a8741ec5-export_snapshot-t1-c4980743",
  "matches": 90530,
  "fingerprint": "690f587012d228d79ec29d095378d54b8f7d69123e477bca4a933bcd3c7dff5f",
  "source_chain": {
    "preflight": {
      "params": {
        "assets_snapshot_id": "web-demo-v1",
        "raw_input": "hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv"
      },
      "source": "hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv"
    },
    "normalize_silver": {
      "report_path": "/mnt/d/BigDataTH_Project/Dota2_BigData/evidence/step2/match_drafts_af-0f492d72a8741ec5-normalize_silver-t1-2c37d1c5/report.json",
      "rows": 90530,
      "uri": "hdfs://localhost:9000/dota/processed/match_drafts_af-0f492d72a8741ec5-normalize_silver-t1-2c37d1c5"
    },
    "analyze_gold": {
      "fingerprint": "690f587012d228d79ec29d095378d54b8f7d69123e477bca4a933bcd3c7dff5f",
      "report_path": "/mnt/d/BigDataTH_Project/Dota2_BigData/evidence/step3/af-0f492d72a8741ec5-analyze_gold-t1-d42ea7ec/report.json",
      "run_id": "af-0f492d72a8741ec5-analyze_gold-t1-d42ea7ec",
      "silver_uri": "hdfs://localhost:9000/dota/processed/match_drafts_af-0f492d72a8741ec5-normalize_silver-t1-2c37d1c5"
    },
    "train_lr": {
      "fingerprint": "690f587012d228d79ec29d095378d54b8f7d69123e477bca4a933bcd3c7dff5f",
      "manifest_path": "/mnt/d/BigDataTH_Project/Dota2_BigData/evidence/step4/af-0f492d72a8741ec5-train_lr-t1-90851e0d/manifest.json",
      "run_id": "af-0f492d72a8741ec5-train_lr-t1-90851e0d",
      "silver_uri": "hdfs://localhost:9000/dota/processed/match_drafts_af-0f492d72a8741ec5-normalize_silver-t1-2c37d1c5"
    },
    "export_snapshot": {
      "path": "/mnt/d/BigDataTH_Project/Dota2_BigData/artifacts/web/af-0f492d72a8741ec5-export_snapshot-t1-c4980743",
      "snapshot_id": "af-0f492d72a8741ec5-export_snapshot-t1-c4980743"
    }
  }
}
```
