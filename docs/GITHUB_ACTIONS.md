# GitHub Actions — chạy pipeline và tải kết quả

Mở workflow [Dota 2 — HDFS, Spark ML và Kafka qua Airflow](https://github.com/ncdanh0311/Dota2_BigData/actions/workflows/pipeline.yml).
Workflow có hai job nối tiếp; tên job và bước chạy mô tả luồng xử lý bằng tiếng Việt.
Chạy khi push mã lên main hoặc chọn **Run workflow**. Thay đổi chỉ tài liệu không
chạy lại pipeline lớn. Pull request chạy các bộ fixture để phản hồi nhanh.

## Job batch

1. Cài Spark 4.2.0, Java 17 và Hadoop 3.5.0; kiểm tra SHA-512 của Hadoop.
   Gói Hadoop tải từ CDN Apache, có archive fallback và cache; cache vẫn phải
   khớp SHA-512 trước khi giải nén. Chuẩn bị runtime giới hạn 20 phút.
2. Dựng NameNode và ba DataNode; replication 2, block 64 MiB trên cùng runner.
3. Cài Airflow 3.3.2 trong venv riêng, theo constraints Python 3.11 chính thức.
4. Tải toàn bộ CSV Zenodo, đối chiếu kích thước và SHA-256 trong manifest nguồn.
5. Nạp HDFS, đọc lại checksum, thử dừng một DataNode rồi phục hồi.
6. Trigger DAG với đường dẫn nguồn thiếu để kiểm tra preflight và finalizer thất bại.
7. Trigger `dota_batch_pipeline` thật: preflight → Silver → Gold → Logistic
   Regression → snapshot website → nghiệm thu. Scheduler thực thi task; không gọi
   tuần tự các hàm rồi mô tả chúng là một DAG run.
8. Kiểm tra các báo cáo đọc lại Parquet/model, fingerprint và số dòng; xuất Silver,
   bảng Gold, metrics và snapshot. Mở website bằng Chrome headless để chọn đội
   hình demo, dự đoán và so sánh API với bundle.

Summary hiển thị số snapshot/trận, kết quả Radiant/Dire, số dòng các bảng Gold,
metrics baseline/model và bảng trạng thái/lần thử/thời lượng từng task Airflow.
Các ô được điền bằng output của lượt chạy, không hard-code số liệu WSL cũ.

## Job Kafka

Job này chỉ bắt đầu sau khi batch đạt. Runner mới dựng HDFS/Airflow độc lập và
nhận Silver cùng báo cáo website qua Artifacts của chính lượt workflow đó.
Manifest và SHA-256 từng file phải khớp trước khi nạp lại Silver.

Cài broker Kafka 4.1.2 KRaft thật, xác minh SHA-512 rồi trigger
`dota_replay_demo`: chọn 1.000 trận, gửi vào topic riêng, chủ đích lỗi sau khi
producer ACK đủ dữ liệu ở lần đầu. Airflow retry gửi lại cùng event ID; Spark
Structured Streaming đọc Kafka, lưu checkpoint HDFS và đối chiếu thống kê với
Python độc lập. Nghiệm thu yêu cầu 2.000 records → 1.000 trận duy nhất,
1.000 records trùng, 0 cách ly; producer có hai lần thử.

Summary có bảng records/trận/trùng/cách ly/picks/wins và trạng thái task. Đây là
phát lại dữ liệu lịch sử; Kafka không phải nguồn trận trực tiếp.

## Artifacts ở cùng trang Summary

| Tên gói (hậu tố run ID và attempt) | Nội dung |
|---|---|
| `bang-chung-batch-*` | Log runner/HDFS/Airflow, receipt, kiểm chứng và ảnh trình duyệt batch |
| `bao-cao-phan-tich-*` | JSON/Markdown, Gold Parquet, metrics, bản website có bundle/metadata/ảnh local |
| `silver-parquet-*` | Silver Parquet đã kiểm chứng, dùng lại ở job Kafka |
| `bang-chung-kafka-*` | Log broker/task/streaming, receipt và ảnh replay |
| `bao-cao-kafka-*` | Kết quả replay, trạng thái DAG và kiểm chứng trình duyệt |
| `checkpoint-kafka-*` | Bronze/batches/checkpoint HDFS, catalog và snapshot thống kê local |

Mỗi gói có `ARTIFACT_README.md`, `artifact-manifest.json`: nguồn
`github_integration`, commit, run/attempt, trạng thái job và SHA-256 từng file.
Giữ 14 ngày. Ảnh `airflow-task-report.png` là bảng dựng từ kết quả REST API,
không phải ảnh chụp Graph/Grid của giao diện Airflow. Các ảnh HDFS/website được
chụp từ trang đang chạy bằng trình duyệt.

Khi lỗi vẫn tải log/báo cáo đã sinh; không xuất Silver/checkpoint thành công nếu
job chưa đạt. Thiếu báo cáo nghiệm thu không được hiển thị như một run hoàn tất.
Runtime và database chỉ ở runner tạm, không upload mật khẩu/DB Airflow hoặc
Kafka storage. Log thu thập được che giá trị mật khẩu tự sinh. Dịch vụ được
dừng ở bước `always()` và máy runner được GitHub thu hồi sau job.
Push mới có thể hủy lượt CI cũ cùng nhánh để tránh giữ máy tạm đang kẹt;
cơ chế này không áp dụng cho DAG/dữ liệu WSL đang vận hành local.

## Cấu hình và các bộ kiểm thử

`ci/prepare_runtime.py` tạo cấu hình trong `$RUNNER_TEMP/dota-integration` và
truyền ba biến `DOTA_HDFS_CONFIG`, `DOTA_KAFKA_CONFIG`, `DOTA_AIRFLOW_CONFIG`.
`orchestration/runtime_config.py` chỉ dùng override khi được đặt rõ ràng; WSL
tiếp tục dùng `configs/*-wsl.json`. Không sửa cấu hình/storage WSL hiện tại.
Python CI 3.11 khác Python WSL 3.14; Spark/Java/logic pipeline giữ cùng phiên bản.

Ba workflow fixture vẫn chạy trên push/PR: Spark/ML/web, DAG/runner và kiểm tra
repo. Chúng ghi nguồn `ci_fixture`. Workflow **Bản lưu — xuất bằng chứng WSL
trước đây** chỉ chạy thủ công, ghi `recorded_project`; không nhầm với kết quả
tích hợp `github_integration` vừa chạy.

## Đối chiếu repo tham khảo

Tham khảo cách tổ chức ở [workflow batch](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/blob/b3dc08716141db93839fb0e501072112a0e35807/.github/workflows/pipeline.yml)
và [workflow Kafka](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/blob/b3dc08716141db93839fb0e501072112a0e35807/.github/workflows/kafka-ingest.yml):
tên bước theo nghiệp vụ, chạy dịch vụ thật, Summary bảng số liệu, thu log cả khi
lỗi và tách artifact bằng chứng/báo cáo/Parquet.

Dota dùng một workflow hai job để truyền Silver đã kiểm chứng, chạy dịch vụ
native Linux theo code quản lý có sẵn. Repo tham khảo dùng Docker Compose và
mô phỏng ba ngày; Dota dùng catalog trận và kiểm thử retry/chống trùng. Không
thêm PostgreSQL hay chia ngày giả khi dataset không có start_time.

## Dùng lại website từ gói tải về

Giải nén `bao-cao-phan-tich-*`; thư mục `website/` chứa snapshot hoàn chỉnh.
Đọc `website/manifest.json` để lấy `snapshot_id`, rồi chép nguyên thư mục đó
thành `artifacts/web/<snapshot_id>/` của repo local. Sau khi cài dependency web
theo hướng dẫn bước 5, chạy `commands/start-web.ps1 -SnapshotId <snapshot_id>`.
Server kiểm tra marker và checksum khi mở. Không cần HDFS/Spark/Kafka để xem
thống kê và dự đoán từ snapshot này; tab replay cần thêm trạng thái replay.
