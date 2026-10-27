# Airflow replay — PASS

DAG: `dota_replay_demo`

Run: `step7-replay-retry-v1`

```json
{
  "run_id": "af-9c66c89caca10990-prepare_replay-t1-484e955a",
  "unique_matches": 1000,
  "total_records": 2000,
  "duplicate_records": 1000,
  "quarantined_records": 0,
  "source_chain": {
    "preflight": {
      "params": {
        "demo_fail_once": true,
        "limit": 1000,
        "rate": 5,
        "silver_input": "hdfs://localhost:9000/dota/processed/match_drafts_v1/"
      },
      "source": "hdfs://localhost:9000/dota/processed/match_drafts_v1/"
    },
    "prepare_replay": {
      "catalog_sha256": "335e4c8a9c6b1b2ed672147c221ef1e79ef0ae11f5e8ee4d3170f9821c8842a7",
      "replay_run_id": "af-9c66c89caca10990-prepare_replay-t1-484e955a",
      "rows": 1000
    },
    "produce_events": {
      "acked": 1000,
      "producer_report": "/mnt/d/BigDataTH_Project/Dota2_BigData/evidence/step6/af-9c66c89caca10990-prepare_replay-t1-484e955a/producer-c3e7f122.json",
      "replay_run_id": "af-9c66c89caca10990-prepare_replay-t1-484e955a"
    },
    "consume_available_now": {
      "replay_run_id": "af-9c66c89caca10990-prepare_replay-t1-484e955a",
      "snapshot_path": "/mnt/d/BigDataTH_Project/Dota2_BigData/artifacts/streaming/af-9c66c89caca10990-prepare_replay-t1-484e955a/latest.json"
    }
  }
}
```
