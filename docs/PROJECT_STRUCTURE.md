# Tổ chức dự án và bài học từ repo tham khảo

Nguồn tham khảo:
https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis

Đã đọc README, cấu trúc thư mục, module điều phối pipeline, cấu hình Spark,
kiểm thử WSL và workflow CI tại commit `b3dc08716141db93839fb0e501072112a0e35807`.
Tham khảo tổ chức và cách vận hành; không nhập mã nghiệp vụ Chicago Crime
hoặc dữ liệu của repo đó vào dự án Dota 2.

| Cách làm ở repo tham khảo | Áp dụng cho Dota 2 |
|---|---|
| `pipeline/` chứa job xử lý | Chuyển job chuẩn hóa vào `pipeline/` |
| `scripts/` chứa bridge vận hành | Giữ quản lý HDFS, thêm bridge PowerShell chung |
| `tests/` tách khỏi job thực tế | Chuyển fixture Spark sang `tests/` |
| Cấu hình WSL ngoài mã | `configs/hdfs-wsl.json` cho user và đường dẫn runtime |
| Metadata nguồn và checksum | Manifest ổn định trong `data/reference/` |
| Kiểm chứng output và artifact CI | Giữ evidence nhỏ; workflow chạy rule tests |
| Luồng ingest/analytics/Airflow riêng | Chưa tạo chức năng giả khi bước đó chưa triển khai |

Repo tham khảo bỏ qua toàn bộ `evidence/` và xuất artifact CI. Dota 2 hiện có
bằng chứng chạy thật cần xem trực tiếp khi nộp môn học, nên giữ các báo cáo nhỏ
trong Git, chỉ bỏ log/dump/cache. Đây là lựa chọn có chủ đích, không sao chép
nguyên `.gitignore` của repo tham khảo.

## Xử lý folder evidence ở thư mục cha

Đã copy `D:\BigDataTH_Project\evidence` vào repo `Dota2_BigData\evidence` và
đối chiếu SHA-256 từng file. Bản ở thư mục cha được giữ làm bản dự phòng.
Mã runtime xác định root từ vị trí file nên các lần chạy mới ghi báo cáo vào
repo mới. Manifest pipeline được đọc từ `data/reference/source_manifest.json`.

HDFS storage vẫn ở `/home/danh/dota-hdfs`, không đổi NameNode, block hoặc UUID.
Không cần tải lại dataset hoặc chạy lại chuẩn hóa chỉ vì đổi cấu trúc folder.

## CI hiện tại

Workflow `Spark rule checks` cài dependency, kiểm tra cú pháp Python và chạy
22 dòng fixture. Không yêu cầu WSL/HDFS trên runner. Artifact chỉ là kết quả
rule tests. Toàn bộ pipeline thật vẫn chạy trên WSL qua PowerShell.

Chưa có CI toàn cụm, tự động phân tích, Kafka hoặc Airflow. Chỉ thêm các luồng
đó sau khi triển khai và kiểm chứng từng bước trên dự án Dota 2.
