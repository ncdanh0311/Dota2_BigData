# Kiểm chứng đổi tên file và rà soát đề xuất — 28/10/2026

Đổi 30 tên file theo chức năng, cập nhật import, PowerShell bridge, đường dẫn
job, test, workflow và hướng dẫn. Không đổi ID/path dữ liệu hoặc chạy lại toàn
bộ dataset để tạo kết quả ML mới.

Kết quả local sau thay đổi:

- `scripts/run-tests.ps1` exit 0: Spark normalization/Gold, ML, web export/API/JS,
  streaming checkpoint, runner, parse DAG và kiểm thử CI summary.
- 56 tham chiếu file được lệnh vận hành gọi, 87 import nội bộ: hợp lệ.
- 3 YAML workflow, 20 bước bash, 20 file PowerShell và PowerShell inline: parse đạt.
- Ignore runtime Airflow lồng thư mục, file credential generated, notebook cache,
  Spark event logs; giữ workflow, DAG, config và evidence.
- Website đọc snapshot/model/replay thật bằng module tên mới; API khớp bundle
  khi chặn socket, không import Spark/Airflow. Kiểm tra này là ASGI in-process,
  không phải một lần chạy lại DAG batch hoặc HTTP live mới.

Chi tiết: [validation_results.json](validation_results.json) và
[website_snapshot_checks.json](website_snapshot_checks.json). Các báo cáo Step 7
lịch sử được giữ nguyên; kết quả kiểm tra mới nằm trong thư mục maintenance này.

Xem [bảng tên file](../../../docs/FILE_NAME_GUIDE.md) và
[đối chiếu đề xuất/Chicago](../../../docs/PROPOSAL_COMPLETION_REVIEW.md).
