# Tên file và cách chạy theo chức năng

Từ 28/10/2026, tên lệnh mô tả hành động thay cho số bước. Script dùng dấu gạch ngang; module Python dùng dấu gạch dưới.

Các lệnh vận hành được gom vào `commands/`; dependency vào `requirements/` để
root gọn. Gọi từ repo bằng `.\commands\ten-lenh.ps1`; các lệnh tự tìm repo root,
không cần chuyển working directory vào thư mục commands.

| File dependency cũ | Vị trí mới |
|---|---|
| `requirements.txt` | `requirements/spark.txt` |
| `requirements-airflow.txt` | `requirements/airflow.txt` |
| `requirements-streaming.txt` | `requirements/streaming.txt` |
| `requirements-web.txt` | `requirements/web.txt` |
| `requirements-web-test.txt` | `requirements/web-test.txt` |

Các file rõ chức năng như `commands/start-hadoop.ps1`, `commands/predict-draft.ps1`, `commands/start-web.ps1` giữ tên. Không đổi run ID, đường dẫn HDFS, model, checkpoint hoặc evidence lịch sử.

| Tên cũ | Tên mới | Công việc |
|---|---|---|
| `run-step2.ps1` | [`commands/prepare-match-drafts.ps1`](../commands/prepare-match-drafts.ps1) | Chuẩn hóa CSV HDFS thành một dòng đội hình mỗi trận |
| `run-step3.ps1` | [`commands/analyze-hero-statistics.ps1`](../commands/analyze-hero-statistics.ps1) | Tổng hợp thống kê hero, bên và cặp hero bằng Spark SQL |
| `run-step4.ps1` | [`commands/train-draft-model.ps1`](../commands/train-draft-model.ps1) | Huấn luyện và đánh giá mô hình dự đoán đội hình |
| `prepare-step5.ps1` | [`commands/export-web-snapshot.ps1`](../commands/export-web-snapshot.ps1) | Xuất Gold/model và ảnh local để website đọc |
| `setup-step6.ps1` | [`commands/install-kafka.ps1`](../commands/install-kafka.ps1) | Cài Kafka riêng và kiểm tra connector |
| `prepare-step6.ps1` | [`commands/prepare-match-replay.ps1`](../commands/prepare-match-replay.ps1) | Tạo catalog và topic cho phiên phát lại trận |
| `run-step6.ps1` | [`commands/consume-match-events.ps1`](../commands/consume-match-events.ps1) | Chạy Spark Structured Streaming đọc Kafka |
| `setup-step7.ps1` | [`commands/install-airflow.ps1`](../commands/install-airflow.ps1) | Cài Airflow và tạo database/pool riêng |
| `scripts/hdfs_lab.py` | [`scripts/hdfs_cluster_manager.py`](../scripts/hdfs_cluster_manager.py) | Quản lý cụm HDFS và nạp/kiểm chứng dữ liệu |
| `scripts/wsl-common.ps1` | [`scripts/wsl-runtime-bridge.ps1`](../scripts/wsl-runtime-bridge.ps1) | Đọc cấu hình và chuyển đường dẫn Windows sang WSL |
| `scripts/streaming-common.ps1` | [`scripts/kafka-job-bridge.ps1`](../scripts/kafka-job-bridge.ps1) | Chạy job Python/Spark Kafka qua WSL |
| `scripts/verify-step5.py` | [`scripts/verify_web_snapshot.py`](../scripts/verify_web_snapshot.py) | Kiểm chứng website với snapshot/model thật, chặn mạng |
| `scripts/verify-step6.py` | [`scripts/verify_replay_statistics.py`](../scripts/verify_replay_statistics.py) | Đối chiếu thống kê replay với catalog độc lập |
| `scripts/verify-step7-web.py` | [`scripts/verify_orchestrated_website.py`](../scripts/verify_orchestrated_website.py) | Kiểm chứng website từ đầu ra DAG Airflow |
| `streaming/kafka_lab.py` | [`streaming/kafka_broker_manager.py`](../streaming/kafka_broker_manager.py) | Cài, bật/tắt và kiểm tra broker Kafka |
| `streaming/prepare.py` | [`streaming/prepare_replay_catalog.py`](../streaming/prepare_replay_catalog.py) | Đóng băng catalog trận và tạo topic riêng |
| `streaming/producer.py` | [`streaming/publish_match_events.py`](../streaming/publish_match_events.py) | Phát sự kiện trận vào Kafka và ghi nhận ACK |
| `streaming/consumer.py` | [`streaming/consume_match_events.py`](../streaming/consume_match_events.py) | Đọc Kafka bằng Spark Structured Streaming |
| `streaming/sink.py` | [`streaming/replay_batch_processor.py`](../streaming/replay_batch_processor.py) | Ghi batch, chống trùng trận và cập nhật thống kê replay |
| `streaming/common.py` | [`streaming/replay_event_contract.py`](../streaming/replay_event_contract.py) | Định dạng/kiểm tra sự kiện và catalog replay |
| `orchestration/airflow_lab.py` | [`orchestration/airflow_runtime_manager.py`](../orchestration/airflow_runtime_manager.py) | Cài, bật/tắt và kiểm tra Airflow |
| `orchestration/runner.py` | [`orchestration/workflow_job_runner.py`](../orchestration/workflow_job_runner.py) | Chạy job con, nhận receipt và xử lý retry/timeout |
| `orchestration/definitions.py` | [`orchestration/pipeline_dag_definitions.py`](../orchestration/pipeline_dag_definitions.py) | Khai báo task, phụ thuộc và tham số hai DAG |
| `orchestration/control.py` | [`orchestration/airflow_run_control.py`](../orchestration/airflow_run_control.py) | Trigger và xem trạng thái run qua Airflow REST API |
| `orchestration/verify_demo.py` | [`orchestration/verify_pipeline_runs.py`](../orchestration/verify_pipeline_runs.py) | Nghiệm thu batch/replay và run lỗi có chủ đích |
| `tests/test_orchestration.py` | [`tests/test_workflow_job_runner.py`](../tests/test_workflow_job_runner.py) | Kiểm thử receipt, retry, timeout và cache ảnh |
| `tests/test_streaming.py` | [`tests/test_streaming_checkpoint_recovery.py`](../tests/test_streaming_checkpoint_recovery.py) | Kiểm thử dedup và khôi phục checkpoint streaming |
| `.github/workflows/checks.yml` | [`.github/workflows/spark-web-tests.yml`](../.github/workflows/spark-web-tests.yml) | CI fixture Spark, ML, web và streaming |
| `.github/workflows/airflow.yml` | [`.github/workflows/airflow-dag-tests.yml`](../.github/workflows/airflow-dag-tests.yml) | CI runner và parse DAG Airflow |
| `.github/workflows/repository.yml` | [`.github/workflows/repository-quality.yml`](../.github/workflows/repository-quality.yml) | CI JSON, quy tắc file Git và cú pháp PowerShell |

Các lệnh cũ trong evidence là lệnh tại thời điểm nghiệm thu; dùng bảng trên để tìm lệnh hiện tại. Không giữ file alias theo số bước. Các hướng dẫn hiện hành và import/job path đã cập nhật sang tên mới.

## Luồng chạy chính

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\commands\start-hadoop.ps1
$runTag = 'demo-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$silver = "hdfs://localhost:9000/dota/processed/match_drafts_$runTag"
.\commands\prepare-match-drafts.ps1 -OutputPath $silver
.\commands\analyze-hero-statistics.ps1 -InputPath $silver -RunId "gold-$runTag"
.\commands\train-draft-model.ps1 -InputPath $silver -RunId "lr-$runTag"
.\commands\export-web-snapshot.ps1 -GoldRunId "gold-$runTag" -ModelRunId "lr-$runTag" `
  -SnapshotId "web-$runTag" -HeroAssetsSnapshotId web-demo-v1
.\commands\start-web.ps1 -SnapshotId "web-$runTag"
```

Kafka: `commands/install-kafka.ps1` → `commands/start-kafka.ps1` → `commands/prepare-match-replay.ps1` → `commands/consume-match-events.ps1` và `commands/replay-matches.ps1` ở hai terminal. Airflow: `commands/install-airflow.ps1` → `commands/start-airflow.ps1`.

Ví dụ tạo run/output mới để không ghi đè v1. Raw CSV cần đã được nạp vào HDFS.
`-HeroAssetsSnapshotId web-demo-v1` dùng cache có sẵn trên máy hiện tại; máy mới
chưa có snapshot thì bỏ tham số này để exporter tải metadata/ảnh theo cơ chế cũ.
