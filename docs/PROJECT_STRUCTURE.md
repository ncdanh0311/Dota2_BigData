# Cấu trúc và cách vận hành dự án

Tên file mô tả công việc. Xem [bảng tên cũ → mới](FILE_NAME_GUIDE.md) và
[đối chiếu đề xuất](PROPOSAL_COMPLETION_REVIEW.md). Số bước chỉ dùng để đọc lộ
trình và tìm bằng chứng, không thay tên chức năng của lệnh chạy.

| Thư mục | Nội dung và điểm vào |
|---|---|
| `pipeline/` | `prepare_match_drafts.py`: CSV snapshot → Silver một trận một dòng |
| `analytics/` | `draft_stats.py`: Silver → Gold thống kê hero, bên và cặp |
| `ml/` | `train_draft_model.py`, `predict_draft.py`: train LR và inference độc lập |
| `web/` | `export_snapshot.py`, `app.py`, `serve.py`: snapshot, API và web server |
| `streaming/` | `prepare_replay_catalog.py`, `publish_match_events.py`, `consume_match_events.py`: catalog → Kafka → thống kê |
| `orchestration/` | `pipeline_dag_definitions.py`, `workflow_job_runner.py`: DAG gọi job, receipt, retry và timeout |
| `orchestration/dags/` | Hai DAG ID ổn định: `dota_batch_pipeline`, `dota_replay_demo` |
| `scripts/` | HDFS manager, bridge WSL, kiểm thử và nghiệm thu |
| `configs/` | Runtime WSL/HDFS/Kafka/Airflow và Spark connector |
| `data/reference/` | Manifest/schema nguồn; không chứa dataset CSV lớn |
| `tests/` | Fixture tính tay, API, model save/load và checkpoint recovery |
| `evidence/` | Báo cáo/ảnh nhỏ, phân biệt fixture với run thật |
| `artifacts/` | Snapshot, bundle, log/cache và output local; Git bỏ qua |
| `.github/workflows/` | CI Spark/web, Airflow và chất lượng repo tách biệt |

## Dữ liệu và runtime

Raw, Parquet và model native ở HDFS. Runtime HDFS/Kafka/Airflow ở filesystem
Linux của WSL theo `configs/`. Website đọc JSON local, không gọi Spark/HDFS trong
request. `PROJECT` xác định từ vị trí module; đổi tên file không đổi root dự án.

Không đổi NameNode/block UUID, run ID, đường dẫn HDFS, topic/catalog, checkpoint
hoặc checksum artifact cũ. Source path trong evidence là đường dẫn tại thời điểm
chạy; dùng bảng tên file để tìm lệnh hiện tại. Không cần nạp lại dataset để đổi tên.

Evidence ở thư mục cha `D:\BigDataTH_Project\evidence` từng được copy và đối
chiếu SHA-256 vào repo, giữ làm dự phòng. Các run mới ghi vào evidence trong repo.
Manifest nguồn vẫn đọc từ `data/reference/source_manifest.json`.

## CI hiện tại

| Workflow | Thực hiện |
|---|---|
| `spark-web-tests.yml` | Spark/ML fixture, API, JavaScript, export snapshot và streaming recovery |
| `airflow-dag-tests.yml` | Runner receipt/retry/timeout, cache offline và parse DAG bằng SDK pin |
| `repository-quality.yml` | JSON, quy tắc file Git, giới hạn file, PowerShell syntax và test summary |

CI dọn báo cáo fixture cũ trước khi chạy, tee log với bash pipefail, viết Job
Summary và upload artifact 14 ngày. Missing/unreadable report không được coi là
PASS. CI không dựng HDFS/Kafka hoặc xử lý toàn bộ 90.530 trận; bằng chứng WSL thật
nằm riêng trong evidence. Chi tiết so sánh với Chicago và lý do giữ evidence
nhỏ trong Git ở [báo cáo đối chiếu](PROPOSAL_COMPLETION_REVIEW.md).
