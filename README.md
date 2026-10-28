# Dota 2 Big Data — Phân tích và dự đoán đội hình

Đồ án cá nhân môn Thực hành Big Data: HDFS, Spark SQL/ML, Kafka/Structured
Streaming, Airflow và website DotaLens chọn 10 hero để dự đoán trước trận.

## Kết quả đã kiểm chứng

| Hạng mục | Kết quả |
|---|---|
| Nguồn | [Zenodo v3](https://zenodo.org/records/23166182), CSV 223 MB, 48 cột |
| Chuẩn hóa | 1.007.768 snapshot → 90.530 trận hợp lệ |
| HDFS | 1 NameNode + 3 DataNode cùng WSL, replication 2 |
| Gold | 127 hero, 254 dòng theo bên, 7.996 cặp cùng đội |
| Logistic Regression | Test accuracy 54,49%; baseline 51,23%; ROC-AUC 0,5589 |
| Streaming | 2.000 records → 1.000 trận duy nhất, 1.000 trùng, 0 cách ly |
| Airflow | Batch và replay chạy thật, có preflight lỗi và producer retry |
| Website | Thống kê, chọn đội hình/dự đoán và tab replay; snapshot local |

F1 Radiant thấp hơn baseline. Dữ liệu lịch sử không đại diện meta hiện tại;
CSV không có patch/start_time. 127 tên hero có metadata; bản demo hiện dùng
ảnh thay thế local do CDN tải ảnh lỗi.

## Cấu trúc gọn

```text
commands/          Lệnh PowerShell: bật/tắt, xử lý, train, replay, mở web
requirements/      Dependency Spark, web, streaming và Airflow tách riêng
configs/           Cấu hình WSL/HDFS/Kafka/Airflow
pipeline/          Chuẩn hóa CSV thành Silver
analytics/         Spark SQL tạo Gold
ml/                Huấn luyện và portable inference
web/               Export snapshot, FastAPI và giao diện
streaming/         Catalog, Kafka producer/consumer và xử lý batch
orchestration/     DAG Airflow, job runner và nghiệm thu
scripts/           Bridge WSL, công cụ quản lý, kiểm thử và đóng gói evidence
tests/             Fixture và kiểm thử tự động
data/reference/    Schema/manifest nguồn, không chứa dataset lớn
docs/              Hướng dẫn vận hành và đối chiếu đề xuất
.github/workflows/ Kiểm thử CI và xuất gói bằng chứng tải về
ci/                Chạy pipeline tích hợp trên GitHub và xuất Summary/Artifacts
```

`evidence/` và `artifacts/` chỉ sinh ở local/CI và được Git bỏ qua. Dataset,
Parquet, checkpoint và model native nằm trong HDFS/local, không đưa vào repo.

## Chạy trên máy đã chuẩn bị WSL

Môi trường đã kiểm chứng: Ubuntu WSL2, Hadoop 3.5.0, Java 17, PySpark 4.2.0,
Python 3.14.4. Runtime dùng cấu hình trong `configs/`; không tự format khi start.

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\commands\start-hadoop.ps1
.\commands\analyze-hero-statistics.ps1
.\commands\train-draft-model.ps1
```

Các job tạo run mới và chặn ghi đè output đã tồn tại. Chuẩn hóa lại thì truyền
OutputPath mới; xem [hướng dẫn tên lệnh và luồng chạy](docs/FILE_NAME_GUIDE.md).

Mở website khi máy đã có snapshot tương ứng, không cần HDFS/Kafka:

```powershell
.\commands\start-web.ps1 -SnapshotId web-demo-v1 -ReplayRunId replay-demo-v1
```

Airflow: `commands/install-airflow.ps1` lần đầu, rồi `commands/start-airflow.ps1`.
Mở [localhost:8080](http://localhost:8080), trigger DAG thủ công sau khi bật dịch
vụ phụ thuộc. Web foreground dừng bằng Ctrl+C; các dịch vụ có lệnh stop riêng.

## Hướng dẫn theo chức năng

| Công việc | Tài liệu |
|---|---|
| Dựng HDFS và nạp dataset | [HDFS_CONFIGURATION.md](docs/HDFS_CONFIGURATION.md) |
| Chuẩn hóa Silver | [STEP2_DATA_PREPARATION.md](docs/STEP2_DATA_PREPARATION.md) |
| Phân tích Gold | [STEP3_DRAFT_ANALYTICS.md](docs/STEP3_DRAFT_ANALYTICS.md) |
| Train và dự đoán | [STEP4_DRAFT_PREDICTION.md](docs/STEP4_DRAFT_PREDICTION.md) |
| Website | [STEP5_WEBSITE.md](docs/STEP5_WEBSITE.md) |
| Kafka replay | [STEP6_KAFKA_STREAMING.md](docs/STEP6_KAFKA_STREAMING.md) |
| Airflow | [STEP7_AIRFLOW.md](docs/STEP7_AIRFLOW.md) |
| Phạm vi đã làm/chưa làm | [PROPOSAL_COMPLETION_REVIEW.md](docs/PROPOSAL_COMPLETION_REVIEW.md) |

MVP theo phạm vi đã chốt đã hoàn thành; RF, temporal/patch analysis và streaming
inference qua hai topic riêng chưa được triển khai. Chi tiết nằm trong báo cáo
đối chiếu, không coi mọi mở rộng của đề xuất là đã hoàn thành.

## Tải evidence từ Actions

Mở [GitHub Actions](https://github.com/ncdanh0311/Dota2_BigData/actions), chọn
lượt chạy và tải ZIP tại **Artifacts**, giống cách xem bằng chứng ở repo tham khảo.

- [`pipeline.yml`](https://github.com/ncdanh0311/Dota2_BigData/actions/workflows/pipeline.yml):
  CSV đầy đủ → HDFS ba DataNode → Airflow → Silver/Gold/ML/website, rồi Kafka
  retry/Structured Streaming. Summary tiếng Việt có bảng số liệu và task;
  tải riêng `bang-chung-*`, `bao-cao-*`, `silver-parquet-*`, `checkpoint-kafka-*`.
- `bang-chung-ci-*`: báo cáo/log fixture do lượt CI vừa chạy sinh ra.
- `bang-chung-do-an-da-ghi-nhan-*`: báo cáo/ảnh đã ghi nhận trước đây, đóng gói
  từ revision gốc và ghi rõ nguồn; không phải CI chạy lại toàn bộ dữ liệu.

Xem [cách tải và phân biệt các gói](docs/EVIDENCE_DOWNLOADS.md). Mỗi ZIP có README,
manifest, nguồn/revision và SHA-256; giữ 14 ngày. Evidence local hiện có được giữ
nguyên khi bỏ theo dõi Git.

Xem [luồng chạy và cách đọc Actions](docs/GITHUB_ACTIONS.md). Kết quả tích hợp
được tạo trên runner; workflow bản lưu WSL chỉ chạy thủ công và ghi rõ nguồn cũ.

## Kiểm thử và dependency

| Workflow | Nội dung |
|---|---|
| `pipeline.yml` | Pipeline tích hợp đầy đủ; batch trước, Kafka nhận Silver cùng run sau |
| `spark-web-tests.yml` | Fixture Spark/ML/web/streaming |
| `airflow-dag-tests.yml` | Runner và parse DAG trong môi trường Airflow riêng |
| `repository-quality.yml` | JSON, file Git, PowerShell và đóng gói evidence |
| `publish-recorded-evidence.yml` | Xuất lại bản lưu báo cáo/ảnh để tải từ Actions |

Ba CI fixture chạy khi push main/PR hoặc thủ công; không cần WSL/HDFS/Kafka.
Pipeline tích hợp tự dựng các dịch vụ trên runner khi push mã main hoặc chạy
thủ công; không chạy lại khi chỉ sửa tài liệu.
Lệnh kiểm thử trên Windows: `.\scripts\run-tests.ps1`.

Dependency nằm trong `requirements/spark.txt`, `web.txt`, `web-test.txt`,
`streaming.txt`, `airflow.txt`; `ci.txt` chỉ thêm trình duyệt kiểm chứng trên CI.
Ví dụ: `pip install -r requirements/web.txt`.
Airflow dùng venv riêng và constraints chính thức; không trộn vào môi trường Spark.
