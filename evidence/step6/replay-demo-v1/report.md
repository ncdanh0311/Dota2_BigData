# Bước 6 — Kết quả replay thật

Ngày nghiệm thu 27/10/2026 (Asia/Saigon). Đây là dữ liệu lịch sử phát lại, không
phải kết quả trận mới thu thập. Thống kê được giữ riêng với Gold/model.

| Chỉ tiêu | Kết quả |
|---|---:|
| Catalog / source Silver | 1.000 / 90.530 trận |
| Run | `replay-demo-v1` |
| Kafka | 4.1.2, KRaft một broker, hai partition |
| Topic ID giữ qua restart | `zQOF93VqTfKmMs8lQ0s_WQ` |
| Kafka records sau hai lượt | 2.000 |
| Trận hợp lệ duy nhất | 1.000 |
| Records trùng | 1.000 |
| Records cách ly trong lượt thật | 0 |
| Radiant / Dire thắng | 454 / 546 |
| Hero picks / wins | 10.000 / 5.000 |

Consumer đầu tiên dừng giữa phiên khi đã commit 391 trận. Producer tiếp tục
đến 1.000 ACK. Consumer khởi động lại cùng checkpoint nhận đủ catalog; phép thử
`verification-recovered.json` xác nhận số đếm và tỷ lệ từng hero khớp bộ đếm
Python độc lập. Sau khi dừng/bật Kafka, gửi lại catalog và tiếp tục checkpoint,
`verification-final.json` vẫn có 1.000 trận, tăng đúng 1.000 records trùng.
Offset partition 0 nối từ 514 sang 515; partition 1 nối từ 484 sang 485.

Lượt đầu 5 trận/giây mất 204,40 giây; lượt duplicate 50/giây mất 24,78 giây.
Snapshot cuối batch 11. `batch_measurements.json` ghi các batch và thời gian
đo thực tế; có batch vượt trigger 10 giây. Không cam kết cập nhật đúng 3/10 giây.

RAM khi HDFS/Kafka/Spark/web đồng thời: WSL tổng 5.774,2 MiB, khả dụng
2.456,7 MiB và không dùng swap, chi tiết ở `runtime_memory.json`.

Fixture riêng `../rule_tests.json` thử JSON/schema/catalog lỗi, quarantine,
crash sau archive commit, checkpoint restart, retry cùng batch và khôi phục
snapshot; `../api_tests.json` thử API chờ/cũ/lỗi/last-good. Các số fixture không
được dùng thay kết quả Kafka thật ở bảng trên.

Website đọc JSON local có checksum; sau khi consumer dừng vẫn hiển thị kết quả
cuối. Gold 90.530 trận và dự đoán ML không thay đổi. Ảnh chụp và browser checks
trong thư mục này ghi lại trạng thái thực tế.

Tại thời điểm thu bằng chứng, thay đổi bước 6 được giữ local, chưa commit/push;
workflow mới cũng chưa chạy trên GitHub. Các cờ trong JSON lưu trạng thái lịch sử
đó; kết quả CI sau khi công bố được theo dõi riêng trong Actions theo commit.
