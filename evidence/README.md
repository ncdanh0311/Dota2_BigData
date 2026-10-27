# Bằng chứng chạy thực tế

- `hdfs/RESULTS.md`: cấu hình, block, replication và kiểm chứng checksum.
- `hdfs/*checksum.json`: đối chiếu nguồn/HDFS trước và sau các phép thử.
- `hdfs/*fsck.txt`, `*report.txt`: kiểm chứng DataNode và block.
- `step2/report.md`, `report.json`: báo cáo chất lượng toàn bộ dataset.
- `step2/sample_matches.json`: mười trận mẫu đã chuẩn hóa.
- `step2/rule_tests.json`: kiểm thử quy tắc với dữ liệu nhỏ.
- `step2/overwrite_guard.json`: phép thử chặn ghi đè output đã có.
- `step3/rule_tests.json`: kiểm thử công thức Gold trên 199 trận tính tay.
- `step3/step3-20261026-v2/`: báo cáo Gold đã đọc lại và có marker hoàn tất trên HDFS.
- `step3/invalidated_runs.json`: ghi chú run đầu bị rút lại vì thiếu inventory;
  dữ liệu chẩn đoán local nằm trong `artifacts/` được Git bỏ qua.
- `step4/*_tests.json`: kiểm thử feature, training và portable inference bằng fixture.
- `step4/step4-20261027-v1/`: kết quả Logistic Regression trên dataset thật, ba
  cấu hình validation, baseline, test, parity và mười ví dụ dự đoán.
- `step4/end_to_end_checks.json`, `overwrite_guard.json`: hash/marker và chống ghi đè run.
- `step5/*_tests.json`: fixture exporter, API và quy tắc hiển thị JavaScript.
- `step5/web-demo-v1/`: export Gold/model thật, trạng thái ảnh, runtime không
  cần HDFS/mạng và kiểm chứng browser desktop/mobile. Bản demo dùng 127 ảnh
  thay thế local do CDN Steam lỗi kết nối; bốn PNG ghi đúng trạng thái đó.
- `step6/rule_tests.json`, `api_tests.json`: fixture streaming/checkpoint và API.
- `step6/replay-demo-v1/`: replay Kafka thật, dừng/khôi phục, phát lại chống trùng,
  đối chiếu catalog độc lập, progress/RAM và ảnh giao diện.
- `step7/runner_tests.json`, `dag_tests.json`: fixture runner/cache ảnh và parse
  bằng SDK Airflow; không cần HDFS/Kafka.
- `step7/RESULTS.md`, `acceptance.json`: nghiệm thu các DAG chạy thật; gồm run
  preflight cố ý thất bại khi HDFS tắt, batch 90.530 trận và Kafka retry.
- `step7/af-*/`: trạng thái Airflow run/task, báo cáo lineage và ảnh Grid/Graph.
- `step7/resources.json`: mẫu PSS/RAM Linux trong lúc demo; không phải benchmark.
- `step7/web_snapshot_checks.json`: app phục vụ artifact thật do Airflow tạo,
  đối chiếu bundle và chặn truy cập mạng trong bài kiểm tra ASGI.

Các báo cáo lịch sử được giữ nguyên nội dung. Đường dẫn cũ trong báo cáo là
đường dẫn tại thời điểm chạy, không phải cấu hình runtime hiện tại.
Log và dump chẩn đoán vẫn có thể tồn tại local nhưng được `.gitignore` bỏ qua.
Không có dataset thô, khóa API hoặc mật khẩu trong bằng chứng được commit.
