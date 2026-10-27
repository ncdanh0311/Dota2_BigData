# Dota 2 Big Data — HDFS và Spark

Đồ án cá nhân môn Thực hành Big Data: xây dựng nền tảng phân tích đội hình và
dự đoán xác suất thắng của hai đội Radiant/Dire trước trận.

**Đã hoàn thành:** nạp HDFS, chuẩn hóa đội hình, phân tích Spark SQL, mô hình
Logistic Regression, website DotaLens và Kafka replay + Spark Structured Streaming.
Airflow chưa triển khai.

## Kết quả đã kiểm chứng

| Hạng mục | Kết quả |
|---|---|
| Nguồn | [Zenodo v3 — Professional Dota 2](https://zenodo.org/records/23166182) |
| CSV nguồn | 223.002.035 byte, 48 cột, 1.007.768 snapshot |
| Trận riêng biệt | 90.530 |
| HDFS | 1 NameNode + 3 DataNode, block 64 MiB, replication 2 |
| Kiểm chứng lưu trữ | 4 block, 8 replica; HEALTHY; SHA-256 khớp nguồn |
| Bảng đội hình | 90.530 trận hợp lệ, 2 file Parquet Snappy |
| Lỗi phát hiện ở bước 2 | 0 dòng trùng, 0 snapshot lỗi, 0 trận mâu thuẫn |
| Thời gian xử lý bước 2 | Khoảng 114 giây trên môi trường WSL đã ghi nhận |
| Gold bước 3 | 127 hero, 254 dòng hero theo bên, 7.996 cặp cùng đội |
| ML bước 4 | Test accuracy 54,49% (baseline 51,23%); ROC-AUC 0,5589 |
| Website bước 5 | Snapshot Gold/model đã kiểm chứng; API + dashboard và chọn 10 hero |
| Streaming bước 6 | Replay 2.000 records, 1.000 trận duy nhất, 1.000 trùng; không cộng vào Gold |

Xem [kết quả HDFS](evidence/hdfs/RESULTS.md) và
[báo cáo chất lượng dữ liệu](evidence/step2/report.md).
[Báo cáo phân tích Gold](evidence/step3/step3-20261026-v2/report.md) có các
bảng top 20 và so sánh năm hero được chọn nhiều nhất.
[Báo cáo ML](evidence/step4/step4-20261027-v1/report.md) chứa ba cấu hình
validation, baseline và test metrics. F1 lớp Radiant thấp hơn baseline;
không khẳng định mô hình tốt hơn trên mọi metric.
Đây là kết quả chạy thật trên WSL; GitHub Actions chỉ chạy kiểm thử quy tắc
bằng dữ liệu nhỏ, không tái tạo toàn bộ cụm HDFS hoặc xử lý CSV 223 MB.

## Luồng dữ liệu hiện tại

```mermaid
flowchart LR
    Z[Zenodo CSV v3] --> R[HDFS /dota/raw]
    R --> V[PySpark: kiểm tra snapshot và trận]
    V --> P[HDFS /dota/processed: đội hình Parquet]
    V --> Q[Cách ly nếu có lỗi]
    P --> S[Spark SQL: hero, bên và cặp cùng đội]
    S --> G[Gold Parquet có version và kiểm chứng]
    G --> E[Báo cáo JSON và Markdown]
    P --> M[Spark ML: train/validation/test theo hash]
    M --> B[Native model và JSON bundle đã đối chiếu]
    B --> D[CLI chọn 10 hero và dự đoán]
  G --> W[Snapshot JSON local đã kiểm chứng]
  B --> W
  W --> API[FastAPI: dashboard và dự đoán]
  API --> UI[Website DotaLens]
  P --> C[Catalog replay bất biến]
  C --> K[Python producer → Kafka]
  K --> SS[Spark Structured Streaming]
  SS --> SA[HDFS batch archive + thống kê replay]
  SA --> SJ[JSON local thay nguyên tử]
  SJ --> API
    E --> A[HDFS /dota/analytics/data_quality]
```

Một snapshot là trạng thái tại một phút trong trận. Pipeline xác nhận đội hình
và kết quả nhất quán rồi mới gom thành một dòng mỗi trận. Bảng đầu ra chỉ có
`match_id`, `radiant_heroes`, `dire_heroes`, `radiant_win`.

## Cấu trúc repo

```text
configs/hdfs-wsl.json          Cấu hình đường dẫn và user WSL
analytics/                    Spark SQL tạo bảng Gold và báo cáo đội hình
ml/                           Chia tập, mã hóa hero, Spark ML và portable inference
web/                          Export snapshot, API FastAPI và giao diện không cần build
streaming/                    Kafka replay, kiểm tra sự kiện, archive batch và Spark SQL
data/reference/                Manifest nguồn và schema, không chứa CSV lớn
docs/                         Hướng dẫn HDFS, chuẩn hóa và cách tổ chức dự án
evidence/hdfs/                Bằng chứng lưu trữ, checksum và chịu lỗi tiến trình
evidence/step2/               Báo cáo chuẩn hóa, mẫu trận và kết quả kiểm thử
evidence/step3/               Thống kê Gold, fixture tests và kiểm chứng
evidence/step4/               Metric, split audit, parity và ví dụ dự đoán
evidence/step5/               Export, API, browser và ảnh demo desktop/mobile
pipeline/                     Spark job chuẩn hóa dữ liệu
scripts/                      Quản lý HDFS, bridge WSL và lệnh kiểm thử
tests/                        Fixture và kiểm thử quy tắc chuẩn hóa
.github/workflows/            Kiểm thử Spark trên GitHub Actions
start-hadoop.ps1              Bật cụm HDFS
stop-hadoop.ps1               Tắt cụm HDFS, giữ nguyên dữ liệu
run-step2.ps1                 Chạy chuẩn hóa từ PowerShell
run-step3.ps1                 Chạy phân tích Spark SQL từ Silver
run-step4.ps1                 Huấn luyện và kiểm chứng Logistic Regression
predict-draft.ps1             Dự đoán bằng JSON, không khởi động Spark
prepare-step5.ps1             Chuẩn bị snapshot website từ các run hoàn tất
start-web.ps1                 Chạy website local từ snapshot, dừng bằng Ctrl+C
setup-step6.ps1               Cài Kafka riêng và kiểm tra connector, SHA-512
start-kafka.ps1 / stop-kafka.ps1 Bật/tắt Kafka, giữ nguyên dữ liệu
prepare-step6.ps1              Tạo catalog và topic của phiên replay mới
run-step6.ps1                  Consumer Structured Streaming có checkpoint
replay-matches.ps1             Phát lại catalog qua Kafka, mặc định 5 trận/giây
```

## Chạy trên máy hiện có

Môi trường đã kiểm chứng: Windows + Ubuntu WSL2, Hadoop 3.5.0, Java 17,
PySpark 4.2.0, Python 3.14.4, WSL khoảng 5,6 GiB RAM. Hadoop/PySpark cần được
cài sẵn. Repo không tự cài Hadoop hoặc khởi tạo máy WSL mới.

1. Kiểm tra `configs/hdfs-wsl.json`: user, thư mục Hadoop/Java/Spark và cụm HDFS.
2. Mở PowerShell tại thư mục repo:

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\start-hadoop.ps1
```

Giao diện quản trị: [http://localhost:9870](http://localhost:9870).

Trên máy hiện tại, CSV và Parquet v1 đã nằm trong HDFS. Chuyển folder repo
không di chuyển block hoặc metadata trong `/home/danh/dota-hdfs`.

```powershell
# Chạy lại vào tên output mới; mặc định chặn ghi đè v1 đã tồn tại.
.\run-step2.ps1 -OutputPath hdfs://localhost:9000/dota/processed/match_drafts_v2

# Phân tích Silver v1, tự tạo run ID mới (ngưỡng hero/cặp mặc định 100).
.\run-step3.ps1

# Huấn luyện từ Silver v1; lưu model theo run ID mới.
.\run-step4.ps1

# Model đã train trên máy hiện tại; dùng Python + bundle local, không cần Spark.
.\predict-draft.ps1 -RunId step4-20261027-v1 `
  -Radiant "7,38,48,85,106" -Dire "11,86,123,128,136"

# Kiểm thử bằng fixture nhỏ, không ghi bảng HDFS.
.\scripts\run-tests.ps1

# Snapshot đã có trên máy hiện tại; không cần Spark/HDFS khi phục vụ website.
.\start-web.ps1 -SnapshotId web-demo-v1

# Dừng riêng cụm HDFS Dota khi không dùng.
.\stop-hadoop.ps1
```

Nếu PowerShell chặn script, dùng `powershell.exe -NoProfile -ExecutionPolicy
Bypass -File .\start-hadoop.ps1` hoặc thay tên script cần chạy.

Với máy mới, đọc [hướng dẫn HDFS](docs/HDFS_CONFIGURATION.md): tạo cấu hình
riêng bằng `hdfs_lab.py setup`, start, download, ingest, verify; sau đó chạy
`run-step2.ps1`. Không format lại NameNode đã có metadata.

Chi tiết: [chuẩn hóa bằng Spark](docs/STEP2_DATA_PREPARATION.md).
Phân tích: [hướng dẫn bước 3](docs/STEP3_DRAFT_ANALYTICS.md). Chỉ tiêu thụ run
Gold có `_COMPLETED`; các run `_INVALIDATED` hoặc chưa hoàn tất không dùng
cho dashboard.
ML: [hướng dẫn bước 4](docs/STEP4_DRAFT_PREDICTION.md). Bundle nằm trong
`artifacts/models/` local và HDFS, không được commit cùng mã nguồn.
Website: [hướng dẫn bước 5](docs/STEP5_WEBSITE.md), mở tại
[http://localhost:8000](http://localhost:8000). Snapshot `web-demo-v1` có 127 tên
hero; ảnh Steam tải lỗi ở môi trường kiểm tra nên dùng ảnh thay thế local.
Website chạy độc lập sau chuẩn bị dữ liệu; máy clone mới cần tái tạo artifact.
Streaming: [hướng dẫn bước 6](docs/STEP6_KAFKA_STREAMING.md). Mở web với
`-ReplayRunId replay-demo-v1` để xem tab mô phỏng đã kiểm chứng; muốn phiên mới
thì prepare một run ID mới. Broker, HDFS và consumer chỉ cần khi cập nhật luồng;
website vẫn đọc kết quả local sau khi các tiến trình đó dừng.

## Dữ liệu và bằng chứng trên GitHub

- CSV nguồn và Parquet nằm trong WSL/HDFS, không commit vào Git.
- `data/reference/source_manifest.json` là metadata đầu vào mà pipeline đọc;
  nó không phụ thuộc thư mục bằng chứng có được tái tạo hay không.
- Giữ JSON/Markdown/TXT nhỏ trong `evidence` để giảng viên xem kết quả.
- Bỏ qua log, cache, dump chẩn đoán và file tải dở bằng `.gitignore`.
- Báo cáo lịch sử có thể chứa đường dẫn thư mục trước khi repo được sắp xếp;
  các hướng dẫn vận hành đã được cập nhật theo folder mới.

## Lộ trình tiếp theo

Đã chuyển cách tiếp cận từ tài liệu Premier League sang
[kế hoạch phân tích Dota 2](docs/DOTA2_ANALYSIS_PLAN.md): câu hỏi, mẫu số,
ngưỡng mẫu, bốn bảng Gold, version và kiểm chứng. Bước 3 đã triển khai; kết
quả thật nằm trong báo cáo Gold liên kết ở trên.

1. Kiểm thử toàn hệ thống, quay demo và hoàn thiện báo cáo môn học.
2. Airflow nếu cần cho môn học.

Dữ liệu trải nhiều patch và chỉ đến tháng 3/2026. Không dùng các biến trong
trận cho mô hình dự đoán trước trận; nguồn hiện không có patch/start_time.
Ba DataNode cùng WSL chỉ mô phỏng phân tán và lỗi tiến trình trên một máy.
