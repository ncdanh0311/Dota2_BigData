# Bằng chứng chạy thực tế

- `hdfs/RESULTS.md`: cấu hình, block, replication và kiểm chứng checksum.
- `hdfs/*checksum.json`: đối chiếu nguồn/HDFS trước và sau các phép thử.
- `hdfs/*fsck.txt`, `*report.txt`: kiểm chứng DataNode và block.
- `step2/report.md`, `report.json`: báo cáo chất lượng toàn bộ dataset.
- `step2/sample_matches.json`: mười trận mẫu đã chuẩn hóa.
- `step2/rule_tests.json`: kiểm thử quy tắc với dữ liệu nhỏ.
- `step2/overwrite_guard.json`: phép thử chặn ghi đè output đã có.

Các báo cáo lịch sử được giữ nguyên nội dung. Đường dẫn cũ trong báo cáo là
đường dẫn tại thời điểm chạy, không phải cấu hình runtime hiện tại.
Log và dump chẩn đoán vẫn có thể tồn tại local nhưng được `.gitignore` bỏ qua.
Không có dataset thô, khóa API hoặc mật khẩu trong bằng chứng được commit.
